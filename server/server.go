package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"time"
	"yt-indexer/keystore"
	"yt-indexer/utils"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog/log"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type server struct {
	// API endpoints exposed via this http server
	http *http.Server
	// cached config
	config *utils.Config
	// the API keystore
	apiKeyStore keystore.Store
	// keeping track of last fetch of YouTube Data API
	lastFetchTime time.Time

	// elasticsearch client
	esc *elasticsearch.Client
}

func NewServer(conf *utils.Config) (*server, error) {
	s := &server{
		config: conf,
	}

	router := mux.NewRouter()

	getRouter := router.Methods(http.MethodGet).Subrouter()
	getRouter.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong\n"))
	})

	s.http = &http.Server{
		Addr:         net.JoinHostPort(conf.Host, strconv.Itoa(conf.Port)),
		Handler:      router,
		ReadTimeout:  time.Duration(conf.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(conf.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(conf.IdleTimeout) * time.Second,
	}

	// set up a client to the elasticsearch cluster
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: conf.ElasticConfig.Endpoints,
		Username:  conf.ElasticConfig.Username,
		Password:  conf.ElasticConfig.Password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Duration(conf.HttpRequestTimeout) * time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize client to elasticsearch cluster")
	}

	// check client by querying cluster information
	res, err := es.Info()
	if err != nil {
		return nil, errors.Wrap(err, "unable to query elasticsearch cluster information")
	}
	log.Debug().Msg(res.String())
	s.esc = es

	store, err := keystore.NewInMemoryKeyStore(conf.Keys)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create an api keystore")
	}

	s.apiKeyStore = store

	// a random time to bootstrap - 1 hour before now
	s.lastFetchTime = time.Now().UTC().Add(-time.Hour)
	return s, nil
}

func (s *server) RunAsync(ctx context.Context) chan error {
	firstErr := make(chan error, 2)
	// run the async job
	go func(ctx context.Context) {
		fetchEvent := time.NewTicker(time.Duration(s.config.DataFetchInterval) * time.Second)
		for {
			select {
			case <-fetchEvent.C:
				log.Debug().Msg("performing data sync with You Tube API")
				if err := s.QueryYouTubeDataV3API(ctx); err != nil {
					firstErr <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	// run the http server
	go func(ctx context.Context) {
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			firstErr <- err
		}
	}(ctx)

	return firstErr
}

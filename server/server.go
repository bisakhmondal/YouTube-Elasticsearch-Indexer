package server

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"
	"yt-indexer/keystore"
	"yt-indexer/utils"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type server struct {
	http          *http.Server
	config        *utils.Config
	apiKeyStore   keystore.Store
	lastFetchTime time.Time
}

func NewServer(conf *utils.Config) (*server, error) {
	s := &server{
		config: conf,
	}

	router := mux.NewRouter()

	s.http = &http.Server{
		Addr:         net.JoinHostPort(conf.Host, strconv.Itoa(conf.Port)),
		Handler:      router,
		ReadTimeout:  time.Duration(conf.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(conf.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(conf.IdleTimeout) * time.Second,
	}

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

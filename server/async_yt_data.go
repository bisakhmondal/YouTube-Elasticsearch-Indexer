package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// QueryYouTubeDataV3API query the YouTube data v3 API and process the items by indexing it into elasticsearch cluster.
// For HTTP 200 from YouTube API - it processes the items successfully. For HTTP 403 - It performs a key rotation of the
// KeyStore and for other HTTP status code it returns with an error.
func (s *server) QueryYouTubeDataV3API(ctx context.Context) error {

	// query the api, by creating a short context
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(s.config.HttpRequestTimeout)*time.Second)
	defer cancel()
	req, err := s.queryBuilder(reqCtx)
	if err != nil {
		return errors.Wrap(err, "failed to create you tube data query request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "the data query request failed")
	}
	switch resp.StatusCode {
	case http.StatusOK:
		// best scenario
	case http.StatusForbidden:
		// TODO exponential Backoff
		log.Info().Msg("quota exhausted. Rotating api keys to bypass quota limits")
		s.apiKeyStore.Rotate()
	default:
		return fmt.Errorf("unexpexted http response code: %v", resp.StatusCode)
	}

	// update with current time
	s.lastFetchTime = time.Now().UTC()

	// process the contents
	contents, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return errors.Wrap(err, "failed to read to content of the response json")
	}

	return s.processDataItems(ctx, contents)
}

func (s *server) queryBuilder(ctx context.Context) (*http.Request, error) {
	params := url.Values{}
	for k, v := range s.config.QueryParams {
		params.Add(k, v)
	}

	// fetch key from store
	params.Add("key", s.apiKeyStore.Get())

	// add publishedAfter param
	params.Add("publishedAfter", s.lastFetchTime.Format(time.RFC3339))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.config.BaseUrl, nil)
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = params.Encode()
	return req, nil
}

func (s *server) processDataItems(ctx context.Context, contents []byte) error {
	output := new(yTResponseFormat)
	if err := json.Unmarshal(contents, output); err != nil {
		return errors.Wrap(err, "unable to consume json items")
	}

	// format it into a linearize slice
	items := make([]YouTubeDataStoreMetadata, 0, len(output.Items))
	for _, elm := range output.Items {
		items = append(items, YouTubeDataStoreMetadata{
			Title:         elm.Snippet.Title,
			Description:   elm.Snippet.Description,
			ThumbnailsUrl: elm.Snippet.Thumbnails.Default.Url,
			ChannelTitle:  elm.Snippet.ChannelTitle,
			VideoId:       elm.Id.VideoId,
			PublishTime:   elm.Snippet.PublishTime,
			ChannelId:     elm.Snippet.ChannelId,
		})
	}

	return s.indexIntoElasticsearch(ctx, items)
}

// indexIntoElasticsearch push collected data elements from YouTube API to elasticsearch cluster
//
func (s *server) indexIntoElasticsearch(ctx context.Context, items []YouTubeDataStoreMetadata) error {
	// creating a bulk indexer that uses elasticsearch bulk api
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      s.config.ElasticConfig.Index,
		Client:     s.esc,
		NumWorkers: runtime.NumCPU(),
	})
	if err != nil {
		return errors.Wrap(err, "error creating the indexer")
	}

	// Loop over the items collection and add it to the bulk indexer
	for _, item := range items {
		// Preparing the data payload: encode article to JSON
		data, err := json.Marshal(item)
		if err != nil {
			return errors.Wrap(err, "failed to encode article")
		}

		// Add an item to the BulkIndexer
		if err = bi.Add(
			ctx,
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(data),

				// OnFailure is called for each failed operation - log the error
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Error().Err(err).Msg("error consuming the element")
						return
					}

					log.Error().Str("type", res.Error.Type).Str("reason", res.Error.Reason)

				},
			},
		); err != nil {
			return errors.Wrap(err, "unexpected error while adding entry to bulk indexer")
		}
	}

	// perform the data flush
	if err = bi.Close(ctx); err != nil {
		return errors.Wrap(err, "failed to flush the data items")
	}

	// check indexing status
	if stats := bi.Stats(); stats.NumFailed > 0 {
		log.Error().Uint64("count", stats.NumFailed).Msg("failed to index few documents")
	}
	return nil
}

// YouTubeDataStoreMetadata provides a linear single level data to be stored in Elasticsearch index.
type YouTubeDataStoreMetadata struct {
	Title         string `json:"title"`
	Description   string `json:"description"`
	ThumbnailsUrl string `json:"thumbnails"`
	ChannelTitle  string `json:"channelTitle"`

	VideoId     string `json:"videoId"`
	PublishTime string `json:"publishTime"`
	ChannelId   string `json:"channelId"`
}

type yTResponseFormat struct {
	Kind       string `json:"kind"`
	RegionCode string `json:"regionCode"`
	PageInfo   struct {
		TotalResults   int `json:"totalResults"`
		ResultsPerPage int `json:"resultsPerPage"`
	} `json:"pageInfo"`
	Items []struct {
		Id struct {
			Kind    string `json:"kind"`
			VideoId string `json:"videoId"`
		} `json:"id"`
		Snippet struct {
			PublishedAt time.Time `json:"publishedAt"`
			ChannelId   string    `json:"channelId"`
			Title       string    `json:"title"`
			Description string    `json:"description"`
			Thumbnails  struct {
				Default struct {
					Url    string `json:"url"`
					Width  int    `json:"width"`
					Height int    `json:"height"`
				} `json:"default"`
			} `json:"thumbnails"`
			ChannelTitle string `json:"channelTitle"`
			PublishTime  string `json:"publishTime"`
		} `json:"snippet"`
	} `json:"items"`
}

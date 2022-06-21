package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"net/url"
	"time"
)

// QueryYouTubeDataV3API query the YouTube data v3 API and process the items by indexing it into elasticsearch cluster.
// For HTTP 200 from YouTube API - it processes the items successfully. For HTTP 403 - It performs a key rotation of the
// KeyStore and for other HTTP status code it returns with an error.
func (s *server) QueryYouTubeDataV3API(ctx context.Context) error {

	// query the api, by creating a short context
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(s.config.HttpRequestTimeout)*time.Second)
	req, err := s.queryBuilder(reqCtx)
	if err != nil {
		cancel()
		return errors.Wrap(err, "failed to create you tube data query request")
	}

	resp, err := http.DefaultClient.Do(req)
	cancel()
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
	//s.processDataItems(ctx, contents)
	log.Info().Msg(string(contents))

	return nil
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

//func (s *server) processDataItems(ctx context.Context, contents []byte) error {
//	output := new(YTResponse)
//	if err := json.Unmarshal(contents, output); err != nil {
//		return errors.Wrap(err, "unable to consume json items")
//	}
//
//}

type YTResponse struct {
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
			ChannelTitle string    `json:"channelTitle"`
			PublishTime  time.Time `json:"publishTime"`
		} `json:"snippet"`
	} `json:"items"`
}

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
)

// DefaultMaxPaginationSize is the maximum number of entries returned if the query params are not provided in API request.
const (
	DefaultMaxPaginationSize = 25
	searchAllQuery           = `
{
  "query": {
    "match_all": {}
  },
  "sort": [
    {
      "publishTime": {
        "order": "desc"
      }
    }
  ],
  "track_total_hits": true,
  "from": %d,
  "size": %d
}`

	searchMatchQuery = `
{
  "query" : {
		"multi_match" : {
			"query" : %q,
			"fields" : [
			  "title^2",
			  "description"
        ]
		}
	}
}
`
)

// fetchAllPaginated returns the stored video data in a paginated response sorted in descending
// order of published datetime.
// Request formats: (can be passed with 2 query params 1. pagination-from 2. pagination-size )
// GET /api/v1/search/all
// returns first 25 videos data(DefaultMaxPaginationSize)
//
// GET /api/v1/search/all?pagination-from=10
// returns 25 videos data starting from 10+1
//
// GET /api/v1/search/all?pagination-from=100&pagination-size=30
// returns 30 videos data starting from 100+1
//
//
// Response Type: searchDBResponse
func (s *server) fetchAllPaginated(w http.ResponseWriter, r *http.Request) {
	paginationFrom, paginationSize := r.URL.Query().Get("pagination-from"), r.URL.Query().Get("pagination-size")
	var err error
	// start with default if unspecified in query params
	queryFrom, querySize := 0, DefaultMaxPaginationSize
	if paginationFrom != "" {
		queryFrom, err = strconv.Atoi(paginationFrom)
		if err != nil {
			log.Error().Err(err).Msg("failed to convert pagination-from string to integer")
			http.Error(w, "invalid query params value - 'pagination-from' ", http.StatusBadRequest)
			return
		}
	}

	if paginationSize != "" {
		querySize, err = strconv.Atoi(paginationSize)
		if err != nil {
			log.Error().Err(err).Msg("failed to convert pagination-size string to integer")
			http.Error(w, "invalid query params value - 'pagination-size' ", http.StatusBadRequest)
			return
		}
	}

	s.queryElasticsearch(r.Context(), w, fmt.Sprintf(searchAllQuery, queryFrom, querySize))
}

// matchQueryResult returns the stored video data in a searchDBResponse format based on the passed query.
// Request formats: (can be passed with 1 query params 1. q )
// GET /api/v1/search?q="tea how"
// returns matched videos' data
//
//
// Response Type: searchDBResponse
func (s *server) matchQueryResult(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "use the /api/v1/search/all instead or pass a search query in 'q' param", http.StatusBadRequest)
		return
	}

	s.queryElasticsearch(r.Context(), w, fmt.Sprintf(searchMatchQuery, query))
}

// searchDBResponse defines the response schema of api responses
type searchDBResponse struct {
	Count float64                    `json:"total_count,omitempty"`
	Items []YouTubeDataStoreMetadata `json:"items"`
}

// queryElasticsearch is a utility that takes the http responsewriter and the query string (compatible in elasticsearch query format)
// to query the elasticsearch cluster and write it to the response body in searchDBResponse schema.
func (s *server) queryElasticsearch(ctx context.Context, w http.ResponseWriter, query string) {
	resp, err := s.esc.Search(
		s.esc.Search.WithContext(ctx),
		s.esc.Search.WithIndex(s.config.ElasticConfig.Index),
		s.esc.Search.WithBody(
			strings.NewReader(query),
		),
	)

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Error().Err(err).Msg("unable to read queried data")
		http.Error(w, "unable to read queried data", http.StatusInternalServerError)
		return
	}

	returnResponse := &searchDBResponse{
		Count: 0,
		Items: make([]YouTubeDataStoreMetadata, 0),
	}

	for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
		var item YouTubeDataStoreMetadata

		craft := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if err := mapstructure.Decode(craft, &item); err != nil {
			log.Error().Err(err).Msg("unable to decode map structure")
			http.Error(w, "unable to decode map structure", http.StatusInternalServerError)
			return
		}
		returnResponse.Items = append(returnResponse.Items, item)
	}
	if count, err := getFloat(result["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"]); err != nil {
		log.Error().Err(err).Msg("invalid count type in response schema")
	} else {
		returnResponse.Count = count
	}

	responseData, err := json.Marshal(returnResponse)
	if err != nil {
		log.Error().Err(err).Msg("unable to process the response data")
		http.Error(w, "unable to process the response data", http.StatusInternalServerError)
		return
	}
	w.Write(responseData)
}

// getFloat utility to convert numbers to float64
func getFloat(unk interface{}) (float64, error) {
	switch i := unk.(type) {
	case float64:
		return i, nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		return strconv.ParseFloat(i, 64)
	default:
		return 0, nil
	}
}

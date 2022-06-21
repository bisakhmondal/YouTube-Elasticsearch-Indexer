package utils

import (
	"encoding/json"
	"os"
	"yt-indexer/keystore"
)

// default values
const (
	DefaultReadTimeout        = 20
	DefaultWriteTimeout       = 20
	DefaultIdleTimeout        = 120
	DefaultHttpRequestTimeout = 25
	DefaultPort               = 8880
	DefaultHost               = "0.0.0.0"
)

type Config struct {
	// YouTube Query Config
	Keys              []keystore.Key    `json:"keys"`
	QueryParams       map[string]string `json:"query-string"`
	BaseUrl           string            `json:"base-url"`
	DataFetchInterval int               `json:"data-fetch-interval"`

	// HTTP server Config
	Host         string `json:"host"`
	Port         int    `json:"port"`
	ReadTimeout  int    `json:"read-timeout"`
	WriteTimeout int    `json:"write-timeout"`
	IdleTimeout  int    `json:"idle-timeout"`

	// http client config
	HttpRequestTimeout int `json:"generic-http-request-timeout"`

	ElasticConfig struct {
		Endpoints []string `json:"endpoints"`
		Username  string   `json:"username"`
		Password  string   `json:"password"`

		Index string `json:"index"`
	} `json:"elasticsearch-config"`

	// etcd config for leader election
	EtcdConfig struct {
		Endpoints   []string `json:"endpoints"`
		Username    string   `json:"username"`
		Password    string   `json:"password"`
		ElectionKey string   `json:"election-key"`
	} `json:"etcd-config"`
}

// LoadConfig loads config from the json file specified to filePath args.
func LoadConfig(filePath string) (*Config, error) {
	conf := &Config{
		Keys:        make([]keystore.Key, 0),
		QueryParams: make(map[string]string),
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return conf, err
	}
	if err = json.Unmarshal(content, conf); err != nil {
		return nil, err
	}

	// set the defaults
	if conf.Host == "" {
		conf.Host = DefaultHost
	}
	if conf.Port == 0 {
		conf.Port = DefaultPort
	}
	if conf.ReadTimeout == 0 {
		conf.ReadTimeout = DefaultReadTimeout
	}
	if conf.WriteTimeout == 0 {
		conf.WriteTimeout = DefaultWriteTimeout
	}
	if conf.IdleTimeout == 0 {
		conf.IdleTimeout = DefaultIdleTimeout
	}
	if conf.HttpRequestTimeout == 0 {
		conf.HttpRequestTimeout = DefaultHttpRequestTimeout
	}

	return conf, nil
}

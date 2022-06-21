package server

import (
	"context"
	"testing"
	"yt-indexer/utils"
)

func TestNewServer(t *testing.T) {
	conf, err := utils.LoadConfig("../config.json")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal(s.QueryYouTubeDataV3API(context.Background()))
}

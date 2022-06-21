package main

import (
	"context"
	"flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"yt-indexer/server"
	"yt-indexer/utils"
)

var (
	debug      = flag.Bool("debug", true, "to enable debug level logging")
	configFile = flag.String("config", "config.json", "config used for you tube indexer")
)

func init() {
	flag.Parse()
	// setup logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Str("component", "yt-indexer").Timestamp().Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if *configFile == "" {
		log.Fatal().Msg("requires a config file...unable to run application")
	}
}

func main() {
	//load config
	config, err := utils.LoadConfig(*configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config file")
	}

	srv, err := server.NewServer(config)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create a server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := srv.RunAsync(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sig:
		log.Info().Str("signal", s.String()).Msg("signal received...exiting")
	case err := <-errChan:
		log.Error().Err(err).Msg("Failed to run the server")
	case <-ctx.Done():
		log.Error().Msg("Irrecoverable state...exiting")
		os.Exit(1)
	}
}

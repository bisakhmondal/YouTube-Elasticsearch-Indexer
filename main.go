package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"yt-indexer/leaderelection"
	"yt-indexer/server"
	"yt-indexer/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to fetch the hostname")
	}

	srv, err := server.NewServer(config)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create a server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaderElector, err := leaderelection.NewRaftBasedLeaderElector(ctx, config, hostname)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup a leader election mechanism")
	}
	// elector initiates the election in asynchronous fashion
	leaderElector.Campaign()

	errChan := srv.RunAsync(ctx, leaderElector)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	log.Info().Str("host", config.Host).Int("port", config.Port).Msg("server is up and running")

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

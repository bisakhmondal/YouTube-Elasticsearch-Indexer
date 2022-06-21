package leaderelection

import (
	"context"
	"sync/atomic"
	"time"
	"yt-indexer/utils"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/clientv3/concurrency"
)

// this pkg uses a raft based leader election using etcd wrapper

const (
	Leader    int32 = 1
	Candidate       = 0

	NoOngoingElection int32 = 0
	OngoingElection         = 1
)

type RaftLeaderElector struct {
	leadershipStatus int32
	startElection    chan struct{}
	electionStatus   int32

	electionSession *concurrency.Session
	election        *concurrency.Election

	ctx context.Context
}

// NewRaftBasedLeaderElector returns an RaftLeaderElector that implements the Elector interface.
func NewRaftBasedLeaderElector(ctx context.Context, config *utils.Config, hostname string) (Elector, error) {
	elector := &RaftLeaderElector{
		startElection: make(chan struct{}),
		ctx:           ctx,
	}
	atomic.StoreInt32(&elector.leadershipStatus, Candidate)
	atomic.StoreInt32(&elector.electionStatus, NoOngoingElection)

	// Create an etcd client
	cli, err := clientv3.New(
		clientv3.Config{
			Endpoints:   config.EtcdConfig.Endpoints,
			DialTimeout: time.Duration(config.HttpRequestTimeout) * time.Second,
			Username:    config.EtcdConfig.Username,
			Password:    config.EtcdConfig.Password,
			Context:     ctx,
		})

	if err != nil {
		return nil, errors.Wrap(err, "failed to create etcd connection")
	}

	// the go routine responsible for running the election in an event driven fashion
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				// close the cli
				if err := cli.Close(); err != nil {
					log.Error().Err(err).Msg("failed to close the cli")
				}
				return
			case <-elector.startElection:
				atomic.StoreInt32(&elector.electionStatus, OngoingElection)

				log.Debug().Msg("initiating a new session for leader election")
				// running a new session for election
				elector.electionSession, err = concurrency.NewSession(cli)
				if err != nil {
					log.Error().Err(err).Msg("error while continuing leader election")
					continue
				}

				// run the campaign
				elector.election = concurrency.NewElection(elector.electionSession, config.EtcdConfig.ElectionKey)
				if err := elector.election.Campaign(ctx, hostname); err != nil {
					log.Error().Err(err).Msg("failed to elect leader through blocking campaign call")
					continue
				}

				log.Debug().Msg("current node is a leader")
				// at this point the node is the leader
				atomic.StoreInt32(&elector.leadershipStatus, Leader)
			}
		}
	}(ctx)

	return elector, nil
}

func (r *RaftLeaderElector) IsLeader() bool {
	return atomic.LoadInt32(&r.leadershipStatus) == Leader
}

func (r *RaftLeaderElector) Campaign() {
	if atomic.LoadInt32(&r.leadershipStatus) == Leader || atomic.LoadInt32(&r.electionStatus) == OngoingElection {
		return
	}
	r.startElection <- struct{}{}
}

func (r *RaftLeaderElector) Resign() {
	if err := r.election.Resign(r.ctx); err != nil {
		log.Error().Err(err).Msg("failed to resign from the leadership status")
		return
	}

	if err := r.electionSession.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close the current election session")
		return
	}

	// update the election status and leadership status
	atomic.StoreInt32(&r.electionStatus, NoOngoingElection)
	atomic.StoreInt32(&r.leadershipStatus, Candidate)
}

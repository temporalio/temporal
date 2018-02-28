// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"log"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/service/frontend"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
	"github.com/uber/cadence/service/worker"

	"go.uber.org/zap"
)

type (
	server struct {
		name   string
		cfg    *config.Config
		doneC  chan struct{}
		daemon common.Daemon
	}
)

const (
	frontendService = "frontend"
	historyService  = "history"
	matchingService = "matching"
	workerService   = "worker"
)

// newServer returns a new instance of a daemon
// that represents a cadence service
func newServer(service string, cfg *config.Config) common.Daemon {
	return &server{
		cfg:   cfg,
		name:  service,
		doneC: make(chan struct{}),
	}
}

// Start starts the server
func (s *server) Start() {
	if _, ok := s.cfg.Services[s.name]; !ok {
		log.Fatalf("`%v` service missing config", s)
	}
	s.daemon = s.startService()
}

// Stop stops the server
func (s *server) Stop() {

	if s.daemon == nil {
		return
	}

	select {
	case <-s.doneC:
	default:
		s.daemon.Stop()
		select {
		case <-s.doneC:
		case <-time.After(time.Minute):
			log.Printf("timed out waiting for server %v to exit\n", s.name)
		}
	}
}

// startService starts a service with the given name and config
func (s *server) startService() common.Daemon {

	var err error

	params := service.BootstrapParams{}
	params.Name = "cadence-" + s.name
	params.Logger = s.cfg.Log.NewBarkLogger()
	params.CassandraConfig = s.cfg.Cassandra

	params.RingpopFactory, err = s.cfg.Ringpop.NewFactory()
	if err != nil {
		log.Fatalf("error creating ringpop factory: %v", err)
	}

	svcCfg := s.cfg.Services[s.name]
	params.MetricScope = svcCfg.Metrics.NewScope()
	params.RPCFactory = svcCfg.RPC.NewFactory(params.Name, params.Logger)
	params.PProfInitializer = svcCfg.PProf.NewInitializer(params.Logger)
	params.ClusterMetadata = cluster.NewMetadata(
		s.cfg.ClustersInfo.InitialFailoverVersion,
		s.cfg.ClustersInfo.FailoverVersionIncrement,
		s.cfg.ClustersInfo.CurrentClusterName,
		s.cfg.ClustersInfo.ClusterNames,
	)
	// TODO: We need to switch Cadence to use zap logger, until then just pass zap.NewNop
	params.MessagingClient = s.cfg.Kafka.NewKafkaClient(zap.NewNop(), params.Logger, params.MetricScope)

	var daemon common.Daemon

	switch s.name {
	case frontendService:
		daemon = frontend.NewService(&params)
	case historyService:
		daemon = history.NewService(&params)
	case matchingService:
		daemon = matching.NewService(&params)
	case workerService:
		daemon = worker.NewService(&params)
	}

	go execute(daemon, s.doneC)

	return daemon
}

// execute runs the daemon in a separate go routine
func execute(d common.Daemon, doneC chan struct{}) {
	d.Start()
	doneC <- struct{}{}
}

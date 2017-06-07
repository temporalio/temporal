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

package frontend

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

// Service represents the cadence-frontend service
type Service struct {
	stopC  chan struct{}
	params *service.BootstrapParams
}

// NewService builds a new cadence-frontend service
func NewService(params *service.BootstrapParams) common.Daemon {
	return &Service{
		params: params,
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var p = s.params
	var log = p.Logger

	log.Infof("%v starting", common.FrontendServiceName)

	base := service.New(p)

	metadata, err := persistence.NewCassandraMetadataPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}
	metadata = persistence.NewMetadataPersistenceClient(metadata, base.GetMetricsClient())

	visibility, err := persistence.NewCassandraVisibilityPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.VisibilityKeyspace,
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create visiblity manager: %v", err)
	}

	history, err := persistence.NewCassandraHistoryPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.Logger)

	if err != nil {
		log.Fatalf("Creating Cassandra history manager persistence failed: %v", err)
	}

	history = persistence.NewHistoryPersistenceClient(history, base.GetMetricsClient())

	handler, tchanServers := NewWorkflowHandler(base, metadata, history, visibility)
	handler.Start(tchanServers)

	log.Infof("%v started", common.FrontendServiceName)

	<-s.stopC

	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.FrontendServiceName)
}

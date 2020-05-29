// Copyright (c) 2017-2020 Uber Technologies, Inc.
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

package domain

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	failoverWatcherSuite struct {
		suite.Suite
		persistencetests.TestBase

		*require.Assertions
		controller *gomock.Controller

		mockResource    *resource.Test
		mockDomainCache *cache.MockDomainCache
		timeSource      clock.TimeSource
		metadataMgr     persistence.MetadataManager
		watcher         *failoverWatcherImpl
	}
)

func TestFailoverWatcherSuite(t *testing.T) {
	s := new(failoverWatcherSuite)
	suite.Run(t, s)
}

func (s *failoverWatcherSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.TestBase = persistencetests.NewTestBaseWithCassandra(&persistencetests.TestBaseOptions{
		ClusterMetadata: cluster.GetTestClusterMetadata(true, true),
	})
	s.TestBase.Setup()
}

func (s *failoverWatcherSuite) TearDownSuite() {
	s.TestBase.TearDownWorkflowStore()
}

func (s *failoverWatcherSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockResource = resource.NewTest(s.controller, metrics.DomainFailoverScope)
	s.mockDomainCache = s.mockResource.DomainCache
	s.metadataMgr = s.TestBase.MetadataManager
	s.timeSource = s.mockResource.GetTimeSource()
	s.watcher = NewFailoverWatcher(
		s.mockDomainCache,
		s.metadataMgr,
		s.timeSource,
		dynamicconfig.GetDurationPropertyFn(10*time.Second),
		dynamicconfig.GetFloatPropertyFn(0.2),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetLogger(),
	).(*failoverWatcherImpl)
}

func (s *failoverWatcherSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.watcher.Stop()
}

func (s *failoverWatcherSuite) TestCleanPendingActiveState() {
	domainName := uuid.New()
	info := &persistence.DomainInfo{
		ID:          domainName,
		Name:        domainName,
		Status:      persistence.DomainStatusRegistered,
		Description: "some random description",
		OwnerEmail:  "some random email",
		Data:        nil,
	}
	domainConfig := &persistence.DomainConfig{
		Retention:  1,
		EmitMetric: true,
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*persistence.ClusterReplicationConfig{
			{
				s.ClusterMetadata.GetCurrentClusterName(),
			},
		},
	}

	_, err := s.metadataMgr.CreateDomain(&persistence.CreateDomainRequest{
		Info:              info,
		Config:            domainConfig,
		ReplicationConfig: replicationConfig,
		IsGlobalDomain:    true,
		ConfigVersion:     1,
		FailoverVersion:   1,
	})
	s.NoError(err)

	// does not have failover end time
	err = CleanPendingActiveState(s.metadataMgr, domainName, 1, s.watcher.retryPolicy)
	s.NoError(err)

	metadata, err := s.metadataMgr.GetMetadata()
	s.NoError(err)
	notificationVersion := metadata.NotificationVersion
	err = s.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:                        info,
		Config:                      domainConfig,
		ReplicationConfig:           replicationConfig,
		ConfigVersion:               1,
		FailoverVersion:             2,
		FailoverNotificationVersion: notificationVersion,
		FailoverEndTime:             common.Int64Ptr(2),
		NotificationVersion:         notificationVersion,
	})
	s.NoError(err)

	// does not have failover version
	err = CleanPendingActiveState(s.metadataMgr, domainName, 5, s.watcher.retryPolicy)
	s.NoError(err)

	err = CleanPendingActiveState(s.metadataMgr, domainName, 2, s.watcher.retryPolicy)
	s.NoError(err)

	resp, err := s.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: domainName,
	})
	s.NoError(err)
	s.True(resp.FailoverEndTime == nil)
}

func (s *failoverWatcherSuite) TestHandleFailoverTimeout() {
	domainName := uuid.New()
	info := &persistence.DomainInfo{
		ID:          domainName,
		Name:        domainName,
		Status:      persistence.DomainStatusRegistered,
		Description: "some random description",
		OwnerEmail:  "some random email",
		Data:        nil,
	}
	domainConfig := &persistence.DomainConfig{
		Retention:  1,
		EmitMetric: true,
	}
	replicationConfig := &persistence.DomainReplicationConfig{
		ActiveClusterName: s.ClusterMetadata.GetCurrentClusterName(),
		Clusters: []*persistence.ClusterReplicationConfig{
			{
				s.ClusterMetadata.GetCurrentClusterName(),
			},
		},
	}

	_, err := s.metadataMgr.CreateDomain(&persistence.CreateDomainRequest{
		Info:              info,
		Config:            domainConfig,
		ReplicationConfig: replicationConfig,
		IsGlobalDomain:    true,
		ConfigVersion:     1,
		FailoverVersion:   1,
	})
	s.NoError(err)

	metadata, err := s.metadataMgr.GetMetadata()
	s.NoError(err)
	notificationVersion := metadata.NotificationVersion

	endtime := common.Int64Ptr(s.timeSource.Now().UnixNano() - 1)
	err = s.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:                        info,
		Config:                      domainConfig,
		ReplicationConfig:           replicationConfig,
		ConfigVersion:               1,
		FailoverVersion:             2,
		FailoverNotificationVersion: notificationVersion,
		FailoverEndTime:             endtime,
		NotificationVersion:         notificationVersion,
	})
	s.NoError(err)
	domainEntry := cache.NewDomainCacheEntryForTest(
		info,
		domainConfig,
		true,
		replicationConfig,
		2,
		endtime,
		s.ClusterMetadata,
	)
	s.watcher.handleFailoverTimeout(domainEntry)

	resp, err := s.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: domainName,
	})
	s.NoError(err)
	s.True(resp.FailoverEndTime == nil)
}

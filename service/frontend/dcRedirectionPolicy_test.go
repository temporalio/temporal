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
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"os"
	"testing"
)

type (
	noopDCRedirectionPolicySuite struct {
		suite.Suite
		currentClusteName       string
		noopDCRedirectionPolicy *NoopRedirectionPolicy
	}

	forwardingDCRedirectionPolicySuite struct {
		logger bark.Logger
		suite.Suite
		fromDC                        string
		toDC                          string
		mockMetadataMgr               *mocks.MetadataManager
		mockClusterMetadata           *mocks.ClusterMetadata
		forwardingDCRedirectionPolicy *ForwardingDCRedirectionPolicy
	}
)

func TestNoopDCRedirectionPolicySuite(t *testing.T) {
	s := new(noopDCRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *noopDCRedirectionPolicySuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *noopDCRedirectionPolicySuite) TearDownSuite() {

}

func (s *noopDCRedirectionPolicySuite) SetupTest() {
	s.currentClusteName = cluster.TestCurrentClusterName
	s.noopDCRedirectionPolicy = NewNoopRedirectionPolicy(s.currentClusteName)
}

func (s *noopDCRedirectionPolicySuite) TearDownTest() {

}

func (s *noopDCRedirectionPolicySuite) TestGetTargetDatacenter() {
	domainName := "some random domain name"
	domainID := "some random domain ID"

	targetCluster, err := s.noopDCRedirectionPolicy.GetTargetDatacenterByID(domainID)
	s.Nil(err)
	s.Equal(s.currentClusteName, targetCluster)

	targetCluster, err = s.noopDCRedirectionPolicy.GetTargetDatacenterByName(domainName)
	s.Nil(err)
	s.Equal(s.currentClusteName, targetCluster)
}

func TestForwardingDCRedirectionPolicySuite(t *testing.T) {
	s := new(forwardingDCRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *forwardingDCRedirectionPolicySuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *forwardingDCRedirectionPolicySuite) TearDownSuite() {

}

func (s *forwardingDCRedirectionPolicySuite) SetupTest() {
	s.fromDC = cluster.TestCurrentClusterName
	s.toDC = cluster.TestAlternativeClusterName
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	domainCache := cache.NewDomainCache(
		s.mockMetadataMgr,
		s.mockClusterMetadata,
		metrics.NewClient(tally.NoopScope, metrics.Frontend),
		s.logger,
	)
	s.forwardingDCRedirectionPolicy = NewForwardingDCRedirectionPolicy(
		s.fromDC, s.toDC, domainCache,
	)
}

func (s *forwardingDCRedirectionPolicySuite) TearDownTest() {

}

func (s *forwardingDCRedirectionPolicySuite) TestGetTargetDatacenter_LocalDomain() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		IsGlobalDomain: false,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)

	targetCluster, err := s.forwardingDCRedirectionPolicy.GetTargetDatacenterByID(domainID)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)

	targetCluster, err = s.forwardingDCRedirectionPolicy.GetTargetDatacenterByName(domainName)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)
}

func (s *forwardingDCRedirectionPolicySuite) TestGetTargetDatacenter_GlobalDomain_OneReplicationCluster() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		IsGlobalDomain: true,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)

	targetCluster, err := s.forwardingDCRedirectionPolicy.GetTargetDatacenterByID(domainID)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)

	targetCluster, err = s.forwardingDCRedirectionPolicy.GetTargetDatacenterByName(domainName)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)
}

func (s *forwardingDCRedirectionPolicySuite) TestGetTargetDatacenter_GlobalDomain_NoFowarding() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		IsGlobalDomain: true,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)

	targetCluster, err := s.forwardingDCRedirectionPolicy.GetTargetDatacenterByID(domainID)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)

	targetCluster, err = s.forwardingDCRedirectionPolicy.GetTargetDatacenterByName(domainName)
	s.Nil(err)
	s.Equal(s.fromDC, targetCluster)
}

func (s *forwardingDCRedirectionPolicySuite) TestGetTargetDatacenter_GlobalDomain_Fowarding() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: "some other random cluster"},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		IsGlobalDomain: true,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)

	targetCluster, err := s.forwardingDCRedirectionPolicy.GetTargetDatacenterByID(domainID)
	s.Nil(err)
	s.Equal(s.toDC, targetCluster)

	targetCluster, err = s.forwardingDCRedirectionPolicy.GetTargetDatacenterByName(domainName)
	s.Nil(err)
	s.Equal(s.toDC, targetCluster)
}

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
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	noopDCRedirectionPolicySuite struct {
		suite.Suite
		*require.Assertions

		currentClusterName string
		policy             *NoopRedirectionPolicy
	}

	selectedAPIsForwardingRedirectionPolicySuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockDomainCache *cache.MockDomainCache

		domainName             string
		domainID               string
		currentClusterName     string
		alternativeClusterName string
		mockConfig             *Config

		mockClusterMetadata *mocks.ClusterMetadata
		policy              *SelectedAPIsForwardingRedirectionPolicy
	}
)

func TestNoopDCRedirectionPolicySuite(t *testing.T) {
	s := new(noopDCRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *noopDCRedirectionPolicySuite) SetupSuite() {
}

func (s *noopDCRedirectionPolicySuite) TearDownSuite() {

}

func (s *noopDCRedirectionPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.currentClusterName = cluster.TestCurrentClusterName
	s.policy = NewNoopRedirectionPolicy(s.currentClusterName)
}

func (s *noopDCRedirectionPolicySuite) TearDownTest() {

}

func (s *noopDCRedirectionPolicySuite) TestWithDomainRedirect() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithDomainIDRedirect(context.Background(), domainID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithDomainNameRedirect(context.Background(), domainName, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func TestSelectedAPIsForwardingRedirectionPolicySuite(t *testing.T) {
	s := new(selectedAPIsForwardingRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) SetupSuite() {
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TearDownSuite() {

}

func (s *selectedAPIsForwardingRedirectionPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)

	s.domainName = "some random domain name"
	s.domainID = "some random domain ID"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName

	logger, err := loggerimpl.NewDevelopment()
	s.Nil(err)

	s.mockConfig = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), logger), 0, false)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.policy = NewSelectedAPIsForwardingPolicy(
		s.currentClusterName,
		s.mockConfig,
		s.mockDomainCache,
	)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithDomainRedirect_LocalDomain() {
	s.setupLocalDomain()

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithDomainRedirect_GlobalDomain_OneReplicationCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithDomainRedirect_GlobalDomain_NoForwarding_DomainNotWhiltelisted() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithDomainRedirect_GlobalDomain_NoForwarding_APINotWhiltelisted() {
	s.setupGlobalDomainWithTwoReplicationCluster(true, true)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalDomain_Forwarding_CurrentCluster() {
	s.setupGlobalDomainWithTwoReplicationCluster(true, true)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalDomain_Forwarding_AlternativeCluster() {
	s.setupGlobalDomainWithTwoReplicationCluster(true, false)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.alternativeClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalDomain_Forwarding_CurrentClusterToAlternativeCluster() {
	s.setupGlobalDomainWithTwoReplicationCluster(true, true)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return &shared.DomainNotActiveError{
				CurrentCluster: s.currentClusterName,
				ActiveCluster:  s.alternativeClusterName,
			}
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return nil
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalDomain_Forwarding_AlternativeClusterToCurrentCluster() {
	s.setupGlobalDomainWithTwoReplicationCluster(true, false)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return nil
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return &shared.DomainNotActiveError{
				CurrentCluster: s.alternativeClusterName,
				ActiveCluster:  s.currentClusterName,
			}
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithDomainIDRedirect(context.Background(), s.domainID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithDomainNameRedirect(context.Background(), s.domainName, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupLocalDomain() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID, Name: s.domainName},
		&persistence.DomainConfig{Retention: 1},
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.domainID).Return(domainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(s.domainName).Return(domainEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalDomainWithOneReplicationCluster() {
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID, Name: s.domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234, // not used
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.domainID).Return(domainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(s.domainName).Return(domainEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalDomainWithTwoReplicationCluster(forwardingEnabled bool, isRecordActive bool) {
	activeCluster := s.alternativeClusterName
	if isRecordActive {
		activeCluster = s.currentClusterName
	}
	domainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID, Name: s.domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234, // not used
		nil,
	)

	s.mockDomainCache.EXPECT().GetDomainByID(s.domainID).Return(domainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(s.domainName).Return(domainEntry, nil).AnyTimes()
	s.mockConfig.EnableDomainNotActiveAutoForwarding = dynamicconfig.GetBoolPropertyFnFilteredByDomain(forwardingEnabled)
}

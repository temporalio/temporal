// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/api/persistenceblobs/v1"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
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

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache

		namespace              string
		namespaceID            string
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

func (s *noopDCRedirectionPolicySuite) TestWithNamespaceRedirect() {
	namespace := "some random namespace name"
	namespaceID := "some random namespace ID"
	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), namespaceID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), namespace, apiName, callFn)
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
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)

	s.namespace = "some random namespace name"
	s.namespaceID = "deadd0d0-c001-face-d00d-000000000000"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName

	logger, err := loggerimpl.NewDevelopment()
	s.Nil(err)

	s.mockConfig = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), logger), 0, false)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("IsGlobalNamespaceEnabled").Return(true)
	s.policy = NewSelectedAPIsForwardingPolicy(
		s.currentClusterName,
		s.mockConfig,
		s.mockNamespaceCache,
	)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_LocalNamespace() {
	s.setupLocalNamespace()

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_OneReplicationCluster() {
	s.setupGlobalNamespaceWithOneReplicationCluster()

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_NoForwarding_NamespaceNotWhiltelisted() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(false, true)

	apiName := "any random API name"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
	s.Nil(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
	s.Nil(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_NoForwarding_APINotWhiltelisted() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, true)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_CurrentCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, true)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_AlternativeCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.alternativeClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_CurrentClusterToAlternativeCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, true)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return serviceerror.NewNamespaceNotActive("", s.currentClusterName, s.alternativeClusterName)
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return nil
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_AlternativeClusterToCurrentCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return nil
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return serviceerror.NewNamespaceNotActive("", s.alternativeClusterName, s.currentClusterName)
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, callFn)
		s.Nil(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, callFn)
		s.Nil(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupLocalNamespace() {
	namespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: s.namespaceID, Name: s.namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		cluster.TestCurrentClusterName,
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalNamespaceWithOneReplicationCluster() {
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: s.namespaceID, Name: s.namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234, // not used
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalNamespaceWithTwoReplicationCluster(forwardingEnabled bool, isRecordActive bool) {
	activeCluster := s.alternativeClusterName
	if isRecordActive {
		activeCluster = s.currentClusterName
	}
	namespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: s.namespaceID, Name: s.namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234, // not used
		nil,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.mockConfig.EnableNamespaceNotActiveAutoForwarding = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(forwardingEnabled)
}

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

package shard

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
)

type (
	ownershipSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		resource   *resourcetest.Test
		config     *configs.Config
	}
)

func TestOwnershipSuite(t *testing.T) {
	s := new(ownershipSuite)
	suite.Run(t, s)
}

func (s *ownershipSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.resource = resourcetest.NewTest(s.controller, primitives.HistoryService)
	s.config = tests.NewDynamicConfig()

	s.resource.HostInfoProvider.EXPECT().HostInfo().Return(s.resource.GetHostInfo()).AnyTimes()
}

func (s *ownershipSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ownershipSuite) newController(contextFactory ContextFactory) *ControllerImpl {
	return ControllerProvider(
		s.config,
		s.resource.GetLogger(),
		s.resource.GetHistoryServiceResolver(),
		s.resource.GetMetricsHandler(),
		s.resource.GetHostInfoProvider(),
		contextFactory,
	)
}

func (s *ownershipSuite) TestAcquireViaMembershipUpdate() {
	s.config.NumberOfShards = 1
	shardID := int32(1)

	shard := NewMockControllableContext(s.controller)
	shard.EXPECT().GetEngine(gomock.Any()).Return(nil, nil).AnyTimes()
	shard.EXPECT().AssertOwnership(gomock.Any()).Return(nil).AnyTimes()
	shard.EXPECT().IsValid().Return(true).AnyTimes()

	cf := NewMockContextFactory(s.controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).
		DoAndReturn(func(_ int32, _ CloseCallback) (ControllableContext, error) {
			return shard, nil
		})

	s.resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(s.resource.GetHostInfo(), nil).AnyTimes()

	s.resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := s.newController(cf)
	shardController.Start()

	s.Zero(len(shardController.ShardIDs()))

	shardController.ownership.membershipUpdateCh <- &membership.ChangedEvent{}

	s.Eventually(func() bool {
		shardIDs := shardController.ShardIDs()
		return len(shardIDs) == 1 && shardIDs[0] == shardID
	}, 5*time.Second, 100*time.Millisecond)

	s.resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func (s *ownershipSuite) TestAcquireOnDemand() {
	s.config.NumberOfShards = 1
	shardID := int32(1)

	shard := NewMockControllableContext(s.controller)
	cf := NewMockContextFactory(s.controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).Return(shard, nil).Times(1)

	s.resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(s.resource.GetHostInfo(), nil).Times(1)

	s.resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := s.newController(cf)
	shardController.Start()

	_, err := shardController.GetShardByID(shardID)
	s.NoError(err)

	s.resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func (s *ownershipSuite) TestAcquireViaTicker() {
	s.config.NumberOfShards = 1
	s.config.AcquireShardInterval = func() time.Duration {
		return 100 * time.Millisecond
	}

	shardID := int32(1)

	shard := NewMockControllableContext(s.controller)
	shard.EXPECT().GetEngine(gomock.Any()).Return(nil, nil).AnyTimes()
	shard.EXPECT().AssertOwnership(gomock.Any()).Return(nil).AnyTimes()
	shard.EXPECT().IsValid().Return(true).AnyTimes()

	cf := NewMockContextFactory(s.controller)
	cf.EXPECT().CreateContext(shardID, gomock.Any()).Return(shard, nil).Times(1)

	s.resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(s.resource.GetHostInfo(), nil).AnyTimes()

	s.resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	shardController := s.newController(cf)
	shardController.Start()

	time.Sleep(500 * time.Millisecond)
	shardIDs := shardController.ShardIDs()
	s.Len(shardIDs, 1)
	s.Equal(shardID, shardIDs[0])

	s.resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shard.EXPECT().FinishStop().Times(1)
	shardController.Stop()
}

func (s *ownershipSuite) TestAttemptAcquireUnowned() {
	s.config.NumberOfShards = 1
	shardID := int32(1)

	otherHost := "otherHost"
	s.resource.HistoryServiceResolver.EXPECT().
		Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress(otherHost), nil).Times(1)

	s.resource.HistoryServiceResolver.EXPECT().
		AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).
		Return(nil).Times(1)

	cf := NewMockContextFactory(s.controller)
	shardController := s.newController(cf)
	shardController.Start()

	_, err := shardController.GetShardByID(shardID)
	s.Error(err)

	solErr, ok := err.(*serviceerrors.ShardOwnershipLost)
	s.True(ok)
	s.Equal(otherHost, solErr.OwnerHost)
	s.Equal(s.resource.GetHostInfo().Identity(), solErr.CurrentHost)

	s.resource.HistoryServiceResolver.EXPECT().
		RemoveListener(shardControllerMembershipUpdateListenerName).
		Return(nil).Times(1)

	shardController.Stop()
}

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

package queues

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
)

const (
	currentCluster    = "current"
	nonCurrentCluster = "nonCurrent"
)

type (
	executorSuite struct {
		suite.Suite
		*require.Assertions
		ctrl *gomock.Controller

		registry        *namespace.MockRegistry
		activeExecutor  *MockExecutor
		standbyExecutor *MockExecutor
		executor        Executor
	}
)

func TestExecutorSuite(t *testing.T) {
	s := new(executorSuite)
	suite.Run(t, s)
}

func (s *executorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.registry = namespace.NewMockRegistry(s.ctrl)
	s.activeExecutor = NewMockExecutor(s.ctrl)
	s.standbyExecutor = NewMockExecutor(s.ctrl)
	s.executor = NewExecutorWrapper(
		currentCluster,
		s.registry,
		s.activeExecutor,
		s.standbyExecutor,
		log.NewNoopLogger(),
	)
}

func (s *executorSuite) TestExecute_Active() {
	executable := NewMockExecutable(s.ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(nil)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencepb.NamespaceReplicationConfig{
		ActiveClusterName: currentCluster,
		Clusters:          []string{currentCluster},
	}, 1)
	s.registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	s.activeExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, true, nil).Times(1)
	_, isActive, err := s.executor.Execute(context.Background(), executable)
	s.NoError(err)
	s.True(isActive)
}

func (s *executorSuite) TestExecute_Standby() {
	executable := NewMockExecutable(s.ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(nil)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencepb.NamespaceReplicationConfig{
		ActiveClusterName: nonCurrentCluster,
		Clusters:          []string{currentCluster, nonCurrentCluster},
	}, 1)
	s.registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	s.standbyExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, false, nil).Times(1)
	_, isActive, err := s.executor.Execute(context.Background(), executable)
	s.NoError(err)
	s.False(isActive)
}

func (s *executorSuite) TestExecute_Discard() {
	executable := NewMockExecutable(s.ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id").Times(2)
	executable.EXPECT().GetTask().Return(nil)
	executable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencepb.NamespaceReplicationConfig{
		ActiveClusterName: nonCurrentCluster,
		Clusters:          []string{nonCurrentCluster},
	}, 1)
	s.registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	s.activeExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Times(0)
	s.standbyExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Times(0)
	_, isActive, err := s.executor.Execute(context.Background(), executable)

	s.ErrorIs(err, consts.ErrTaskDiscarded)
	s.False(isActive)
}

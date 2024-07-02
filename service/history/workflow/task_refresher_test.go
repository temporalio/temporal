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

package workflow

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	taskRefresherSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry

		namespaceEntry       *namespace.Namespace
		mutableState         MutableState
		stateMachineRegistry *hsm.Registry

		taskRefresher *TaskRefresherImpl
	}
)

func TestTaskRefresherSuite(t *testing.T) {
	s := new(taskRefresherSuite)
	suite.Run(t, s)
}

func (s *taskRefresherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{ShardId: 1},
		config,
	)
	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache

	s.stateMachineRegistry = hsm.NewRegistry()
	s.mockShard.SetStateMachineRegistry(s.stateMachineRegistry)
	s.NoError(RegisterStateMachine(s.stateMachineRegistry))

	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.mockShard.GetLogger(),
		s.namespaceEntry.FailoverVersion(),
		tests.WorkflowID,
		tests.RunID,
	)

	s.taskRefresher = NewTaskRefresher(s.mockShard, s.mockShard.GetLogger())
}

func (s *taskRefresherSuite) TestRefreshSubStateMachineTasks() {

	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	s.NoError(err)
	err = s.stateMachineRegistry.RegisterMachine(stateMachineDef)
	s.NoError(err)

	versionedTransition := &persistence.VersionedTransition{NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(), TransitionCount: 3}
	s.mutableState.GetExecutionInfo().TransitionHistory = []*persistence.VersionedTransition{
		versionedTransition,
	}

	hsmRoot := s.mutableState.HSM()
	child1, err := hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	s.NoError(err)
	_, err = child1.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1_1"}, hsmtest.NewData(hsmtest.State2))
	s.NoError(err)
	_, err = hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
	s.NoError(err)
	// Clear the dirty flag so we can test it later.
	hsmRoot.ClearTransactionState()

	err = s.taskRefresher.refreshTasksForSubStateMachines(s.mutableState)
	s.NoError(err)

	refreshedTasks := s.mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryOutbound], 3)
	s.Len(s.mutableState.GetExecutionInfo().StateMachineTimers, 3)
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)

	s.False(hsmRoot.Dirty())
}

package queues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
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
	t.Parallel()
	s := new(executorSuite)
	suite.Run(t, s)
}

func (s *executorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.registry = namespace.NewMockRegistry(s.ctrl)
	s.activeExecutor = NewMockExecutor(s.ctrl)
	s.standbyExecutor = NewMockExecutor(s.ctrl)
	s.executor = NewActiveStandbyExecutor(
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
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: currentCluster,
		Clusters:          []string{currentCluster},
	}, 1)
	s.registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	s.activeExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        nil,
	}).Times(1)
	resp := s.executor.Execute(context.Background(), executable)
	s.NoError(resp.ExecutionErr)
	s.True(resp.ExecutedAsActive)
}

func (s *executorSuite) TestExecute_Standby() {
	executable := NewMockExecutable(s.ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(nil)
	ns := namespace.NewGlobalNamespaceForTest(nil, nil, &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: nonCurrentCluster,
		Clusters:          []string{currentCluster, nonCurrentCluster},
	}, 1)
	s.registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
	s.standbyExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        nil,
	}).Times(1)
	resp := s.executor.Execute(context.Background(), executable)
	s.NoError(resp.ExecutionErr)
	s.False(resp.ExecutedAsActive)
}

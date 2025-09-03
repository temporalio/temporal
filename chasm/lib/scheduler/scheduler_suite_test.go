package scheduler_test

import (
	"context"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

// schedulerSuite sets up a suite that has a basic CHASM tree ready
// for use with the Scheduler library.
type schedulerSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller    *gomock.Controller
	nodeBackend   *chasm.MockNodeBackend
	specProcessor *scheduler.MockSpecProcessor
	mockEngine    *chasm.MockEngine
	node          *chasm.Node

	scheduler *scheduler.Scheduler

	registry        *chasm.Registry
	timeSource      *clock.EventTimeSource
	nodePathEncoder chasm.NodePathEncoder
	logger          log.Logger

	addedTasks []tasks.Task
}

// SetupSuite initializes the CHASM tree to a default scheduler.
func (s *schedulerSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.addedTasks = make([]tasks.Task, 0)

	s.controller = gomock.NewController(s.T())
	s.nodeBackend = chasm.NewMockNodeBackend(s.controller)
	s.specProcessor = scheduler.NewMockSpecProcessor(s.controller)
	s.mockEngine = chasm.NewMockEngine(s.controller)
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
	s.nodePathEncoder = chasm.DefaultPathEncoder

	s.registry = chasm.NewRegistry(s.logger)
	err := s.registry.Register(&scheduler.Library{})
	s.NoError(err)

	// Advance here, because otherwise ctx.Now().IsZero() will be true.
	s.timeSource = clock.NewEventTimeSource()
	s.timeSource.Update(time.Now())

	// Stub NodeBackend for NewEmptytree
	tv := testvars.New(s.T())
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	// Collect all tasks added for verification.
	//
	// TODO: eventually, when we have more testing framework support for CHASM, we
	// should test the framework-level tasks. The verifications here are a bit lossy,
	// because CHASM's already converted logical tasks to physical tasks during
	// CloseTransaction.
	s.nodeBackend.EXPECT().AddTasks(gomock.Any()).
		Do(func(addedTask tasks.Task) {
			s.addedTasks = append(s.addedTasks, addedTask)
		}).
		AnyTimes()

	s.node = chasm.NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	ctx := s.newMutableContext()
	s.scheduler = scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	s.node.SetRootComponent(s.scheduler)
}

func (s *schedulerSuite) newMutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), s.node)
}

func (s *schedulerSuite) newEngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), s.mockEngine)
}

func (s *schedulerSuite) ExpectReadComponent(returnedComponent chasm.Component) {
	s.mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			chasmCtx := s.newMutableContext()
			return readFn(chasmCtx, returnedComponent)
		}).Times(1)
}

func (s *schedulerSuite) ExpectUpdateComponent(componentToUpdate chasm.Component) {
	s.mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
			chasmCtx := s.newMutableContext()
			err := updateFn(chasmCtx, componentToUpdate)
			return nil, err
		}).Times(1)
}

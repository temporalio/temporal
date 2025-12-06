package scheduler_test

import (
	"context"
	"reflect"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
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
}

// SetupSuite initializes the CHASM tree to a default scheduler.
func (s *schedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.specProcessor = scheduler.NewMockSpecProcessor(s.controller)
	s.mockEngine = chasm.NewMockEngine(s.controller)
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
	s.nodePathEncoder = chasm.DefaultPathEncoder

	s.registry = chasm.NewRegistry(s.logger)
	err := s.registry.Register(&scheduler.Library{})
	s.NoError(err)

	// Register the Core library as well, which we use for Visibility.
	err = s.registry.Register(&chasm.CoreLibrary{})
	s.NoError(err)

	// Advance here, because otherwise ctx.Now().IsZero() will be true.
	s.timeSource = clock.NewEventTimeSource()
	now := time.Now()
	s.timeSource.Update(now)

	// Stub NodeBackend for NewEmptytree
	tv := testvars.New(s.T())
	s.nodeBackend = &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	s.node = chasm.NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	ctx := s.newMutableContext()
	s.scheduler = scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	s.node.SetRootComponent(s.scheduler)
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Advance Generator's high water mark to 'now'.
	generator := s.scheduler.Generator.Get(ctx)
	generator.LastProcessedTime = timestamppb.New(now)

	// Set up future action times.
	futureTime := now.Add(time.Hour)
	s.specProcessor.EXPECT().NextTime(s.scheduler, gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
		Next:    futureTime,
		Nominal: futureTime,
	}, nil).MaxTimes(1)
	s.specProcessor.EXPECT().NextTime(s.scheduler, gomock.Any()).Return(legacyscheduler.GetNextTimeResult{}, nil).AnyTimes()
}

// hasTask returns true if the given task type was added at the end of the
// transaction with the given visibilityTime.
func (s *schedulerSuite) hasTask(task any, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, tasks := range s.nodeBackend.TasksByCategory {
		for _, task := range tasks {
			if reflect.TypeOf(task) == taskType &&
				task.GetVisibilityTime().Equal(visibilityTime) {
				return true
			}
		}
	}

	return false
}

func (s *schedulerSuite) newMutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), s.node)
}

func (s *schedulerSuite) newEngineContext() context.Context {
	return chasm.NewEngineContext(context.Background(), s.mockEngine)
}

func (s *schedulerSuite) ExpectReadComponent(ctx chasm.Context, returnedComponent chasm.Component) {
	s.mockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			return readFn(ctx, returnedComponent)
		}).Times(1)
}

func (s *schedulerSuite) ExpectUpdateComponent(ctx chasm.MutableContext, componentToUpdate chasm.Component) {
	s.mockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
			err := updateFn(ctx, componentToUpdate)
			return nil, err
		}).Times(1)
}

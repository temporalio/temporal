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

package queues_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	executableSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockExecutor          *queues.MockExecutor
		mockScheduler         *queues.MockScheduler
		mockRescheduler       *queues.MockRescheduler
		mockNamespaceRegistry *namespace.MockRegistry
		mockClusterMetadata   *cluster.MockMetadata
		metricsHandler        *metricstest.CaptureHandler

		timeSource *clock.EventTimeSource
	}
	params struct {
		dlqWriter                  *queues.DLQWriter
		dlqEnabled                 dynamicconfig.BoolPropertyFn
		priorityAssigner           queues.PriorityAssigner
		maxUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
		dlqInternalErrors          dynamicconfig.BoolPropertyFn
		dlqErrorPattern            dynamicconfig.StringPropertyFn
	}
	option func(*params)
)

func TestExecutableSuite(t *testing.T) {
	s := new(executableSuite)
	suite.Run(t, s)
}

func (s *executableSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockExecutor = queues.NewMockExecutor(s.controller)
	s.mockScheduler = queues.NewMockScheduler(s.controller)
	s.mockRescheduler = queues.NewMockRescheduler(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.metricsHandler = metricstest.NewCaptureHandler()

	s.mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			ShardCount: 1,
		},
	}).AnyTimes()

	s.timeSource = clock.NewEventTimeSource()
}

func (s *executableSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *executableSuite) TestExecute_TaskExecuted() {
	executable := s.newTestExecutable()

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        errors.New("some random error"),
	})
	s.Error(executable.Execute())

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        nil,
	})
	s.NoError(executable.Execute())
}

func (s *executableSuite) TestExecute_InMemoryNoUserLatency_SingleAttempt() {
	scheduleLatency := 100 * time.Millisecond
	userLatency := 500 * time.Millisecond
	attemptLatency := time.Second
	attemptNoUserLatency := scheduleLatency + attemptLatency - userLatency

	testCases := []struct {
		name                         string
		taskErr                      error
		expectError                  bool
		expectedAttemptNoUserLatency time.Duration
		expectBackoff                bool
	}{
		{
			name:                         "NoError",
			taskErr:                      nil,
			expectError:                  false,
			expectedAttemptNoUserLatency: attemptNoUserLatency,
			expectBackoff:                false,
		},
		{
			name:                         "UnavailableError",
			taskErr:                      serviceerror.NewUnavailable("some random error"),
			expectError:                  true,
			expectedAttemptNoUserLatency: attemptNoUserLatency,
			expectBackoff:                true,
		},
		{
			name:                         "NotFoundError",
			taskErr:                      serviceerror.NewNotFound("not found error"),
			expectError:                  false,
			expectedAttemptNoUserLatency: attemptNoUserLatency,
			expectBackoff:                false,
		},
		{
			name:                         "ResourceExhaustedError",
			taskErr:                      consts.ErrResourceExhaustedBusyWorkflow,
			expectError:                  true,
			expectedAttemptNoUserLatency: 0,
			expectBackoff:                false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {

			executable := s.newTestExecutable()

			now := time.Now()
			s.timeSource.Update(now)
			executable.SetScheduledTime(now)

			now = now.Add(scheduleLatency)
			s.timeSource.Update(now)

			s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Do(func(ctx context.Context, taskInfo interface{}) {
				metrics.ContextCounterAdd(
					ctx,
					metrics.HistoryWorkflowExecutionCacheLatency.Name(),
					int64(userLatency),
				)

				now = now.Add(attemptLatency)
				s.timeSource.Update(now)
			}).Return(queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        tc.taskErr,
			})

			err := executable.Execute()
			if err != nil {
				err = executable.HandleErr(err)
			}

			if tc.expectError {
				s.Error(err)
				s.mockScheduler.EXPECT().TrySubmit(executable).Return(false)
				s.mockRescheduler.EXPECT().Add(executable, gomock.Any())
				executable.Nack(err)
			} else {
				s.NoError(err)
				capture := s.metricsHandler.StartCapture()
				executable.Ack()
				snapshot := capture.Snapshot()
				recordings := snapshot[metrics.TaskLatency.Name()]
				s.Len(recordings, 1)
				actualAttemptNoUserLatency, ok := recordings[0].Value.(time.Duration)
				s.True(ok)
				if tc.expectBackoff {
					// the backoff duration is random, so we can't compare the exact value
					s.Less(tc.expectedAttemptNoUserLatency, actualAttemptNoUserLatency)
				} else {
					s.Equal(tc.expectedAttemptNoUserLatency, actualAttemptNoUserLatency)
				}
			}
		})
	}
}

func (s *executableSuite) TestExecute_InMemoryNoUserLatency_MultipleAttempts() {
	numAttempts := 3
	scheduleLatencies := []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond}
	userLatencies := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	attemptLatencies := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	taskErrors := []error{
		serviceerror.NewUnavailable("test unavailable error"),
		consts.ErrResourceExhaustedBusyWorkflow,
		nil,
	}
	expectedInMemoryNoUserLatency := scheduleLatencies[0] + attemptLatencies[0] - userLatencies[0] +
		scheduleLatencies[2] + attemptLatencies[2] - userLatencies[2]
	_ = expectedInMemoryNoUserLatency

	executable := s.newTestExecutable()

	now := time.Now()
	s.timeSource.Update(now)
	executable.SetScheduledTime(now)

	for i := 0; i != numAttempts; i++ {
		now = now.Add(scheduleLatencies[i])
		s.timeSource.Update(now)

		s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Do(func(ctx context.Context, taskInfo interface{}) {
			metrics.ContextCounterAdd(
				ctx,
				metrics.HistoryWorkflowExecutionCacheLatency.Name(),
				int64(userLatencies[i]),
			)

			now = now.Add(attemptLatencies[i])
			s.timeSource.Update(now)
		}).Return(queues.ExecuteResponse{
			ExecutionMetricTags: nil,
			ExecutedAsActive:    true,
			ExecutionErr:        taskErrors[i],
		})

		err := executable.Execute()
		if err != nil {
			err = executable.HandleErr(err)
		}

		if taskErrors[i] != nil {
			s.Error(err)
			s.mockScheduler.EXPECT().TrySubmit(executable).Return(true)
			executable.Nack(err)
		} else {
			s.NoError(err)
			capture := s.metricsHandler.StartCapture()
			executable.Ack()
			snapshot := capture.Snapshot()
			recordings := snapshot[metrics.TaskLatency.Name()]
			s.Len(recordings, 1)
			actualInMemoryNoUserLatency, ok := recordings[0].Value.(time.Duration)
			s.True(ok)
			s.Equal(expectedInMemoryNoUserLatency, actualInMemoryNoUserLatency)
		}
	}
}

func (s *executableSuite) TestExecute_CapturePanic() {
	executable := s.newTestExecutable()

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic("test panic during execution")
		},
	)
	s.Error(executable.Execute())
}

func (s *executableSuite) TestExecute_CallerInfo() {
	executable := s.newTestExecutable()

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
			s.Equal(headers.CallerTypeBackground, headers.GetCallerInfo(ctx).CallerType)
			return queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        nil,
			}
		},
	)
	s.NoError(executable.Execute())

	executable = s.newTestExecutable(func(p *params) {
		p.priorityAssigner = queues.NewStaticPriorityAssigner(ctasks.PriorityLow)
	})
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
			s.Equal(headers.CallerTypePreemptable, headers.GetCallerInfo(ctx).CallerType)
			return queues.ExecuteResponse{
				ExecutionMetricTags: nil,
				ExecutedAsActive:    true,
				ExecutionErr:        nil,
			}
		},
	)
	s.NoError(executable.Execute())
}

func (s *executableSuite) TestExecuteHandleErr_ResetAttempt() {
	executable := s.newTestExecutable()
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    true,
		ExecutionErr:        errors.New("some random error"),
	})
	err := executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	s.Equal(2, executable.Attempt())

	// isActive changed to false, should reset attempt
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        nil,
	})
	s.NoError(executable.Execute())
	s.Equal(1, executable.Attempt())
}

func (s *executableSuite) TestExecuteHandleErr_Corrupted() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	).Times(2)
	err := executable.Execute()
	s.Error(err)
	s.NoError(executable.HandleErr(err))
	s.Error(executable.Execute())
}

func (s *executableSuite) TestExecute_SendToDLQAfterMaxAttempts() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 2
		}
	})
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        errors.New("some random error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	s.Error(executable.HandleErr(err))

	// Attempt 2
	err = executable.Execute()
	err2 := executable.HandleErr(err)
	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.NoError(executable.Execute())
	s.Len(queueWriter.EnqueueTaskRequests, 1)
}

func (s *executableSuite) TestExecute_DontSendToDLQAfterMaxAttemptsDLQDisabled() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        errors.New("some random error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	s.Error(executable.HandleErr(err))

	// Attempt 2
	s.Error(executable.Execute())
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_DontSendToDLQAfterMaxAttemptsExpectedError() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr: &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "test",
		},
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	s.Error(executable.HandleErr(err))

	// Attempt 2
	s.Error(executable.Execute())
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendToDLQAfterMaxAttemptsThenDisableDropCorruption() {
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})
	execError := serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, errors.New("random error"))
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), execError.Error())

	dlqEnabled = false
	s.NoError(executable.Execute())
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendToDLQAfterMaxAttemptsThenDisable() {
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.maxUnexpectedErrorAttempts = func() int {
			return 1
		}
	})
	execError := errors.New("some random error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)

	dlqEnabled = false

	// Make sure task is retried this time
	execError2 := errors.New("some other random error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError2,
	}).Times(1)
	s.ErrorIs(executable.Execute(), execError2)
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendsInternalErrorsToDLQ_WhenEnabled() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqInternalErrors = func() bool {
			return true
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        serviceerror.NewInternal("injected error"),
	}).Times(1)

	err := executable.HandleErr(executable.Execute())
	s.ErrorIs(err, queues.ErrTerminalTaskFailure)
	s.NoError(executable.Execute())
	s.Len(queueWriter.EnqueueTaskRequests, 1)
}

func (s *executableSuite) TestExecute_DoesntSendInternalErrorsToDLQ_WhenDisabled() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqInternalErrors = func() bool {
			return false
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        serviceerror.NewInternal("injected error"),
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	s.Error(executable.HandleErr(err))

	// Attempt 2
	err = executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendInternalErrorsToDLQ_ThenDisable() {
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.dlqInternalErrors = func() bool {
			return true
		}
	})

	injectedErr := serviceerror.NewInternal("injected error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        injectedErr,
	}).Times(2)

	s.ErrorIs(executable.HandleErr(executable.Execute()), queues.ErrTerminalTaskFailure)

	// The task should be dropped but not sent to DLQ
	dlqEnabled = false
	s.ErrorIs(executable.Execute(), injectedErr)
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_DLQ() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	s.NoError(executable.Execute())
	s.Len(queueWriter.EnqueueTaskRequests, 1)
}

func (s *executableSuite) TestExecute_DLQThenDisable() {
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	dlqEnabled = false
	s.NoError(executable.Execute())
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_DLQFailThenRetry() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
	})

	capture := s.metricsHandler.StartCapture()
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(
		func(_ context.Context, _ queues.Executable) queues.ExecuteResponse {
			panic(serialization.NewUnknownEncodingTypeError("unknownEncoding", enumspb.ENCODING_TYPE_PROTO3))
		},
	)
	err := executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	queueWriter.EnqueueTaskErr = errors.New("some random error")
	err = executable.Execute()
	s.Error(err)
	s.Error(executable.HandleErr(err))
	queueWriter.EnqueueTaskErr = nil
	err = executable.Execute()
	s.NoError(err)
	snapshot := capture.Snapshot()
	s.Len(snapshot[metrics.TaskTerminalFailures.Name()], 1)
	s.Len(snapshot[metrics.TaskDLQFailures.Name()], 1)
	s.Len(snapshot[metrics.TaskDLQSendLatency.Name()], 2)
}

func (s *executableSuite) TestHandleErr_EntityNotExists() {
	executable := s.newTestExecutable()

	s.NoError(executable.HandleErr(serviceerror.NewNotFound("")))
}

func (s *executableSuite) TestHandleErr_ErrTaskRetry() {
	executable := s.newTestExecutable()

	s.Equal(consts.ErrTaskRetry, executable.HandleErr(consts.ErrTaskRetry))
}

func (s *executableSuite) TestHandleErr_ErrDeleteOpenExecution() {
	executable := s.newTestExecutable()

	s.Equal(consts.ErrDependencyTaskNotCompleted, executable.HandleErr(consts.ErrDependencyTaskNotCompleted))
}

func (s *executableSuite) TestHandleErr_ErrTaskDiscarded() {
	executable := s.newTestExecutable()

	s.NoError(executable.HandleErr(consts.ErrTaskDiscarded))
}

func (s *executableSuite) TestHandleErr_ErrTaskVersionMismatch() {
	executable := s.newTestExecutable()

	s.NoError(executable.HandleErr(consts.ErrTaskVersionMismatch))
}

func (s *executableSuite) TestHandleErr_NamespaceNotActiveError() {
	err := serviceerror.NewNamespaceNotActive("", "", "")

	s.Equal(err, s.newTestExecutable().HandleErr(err))
}

func (s *executableSuite) TestHandleErr_RandomErr() {
	executable := s.newTestExecutable()

	s.Error(executable.HandleErr(errors.New("random error")))
}

func (s *executableSuite) TestTaskAck() {
	executable := s.newTestExecutable()

	s.Equal(ctasks.TaskStatePending, executable.State())

	executable.Ack()
	s.Equal(ctasks.TaskStateAcked, executable.State())
}

func (s *executableSuite) TestTaskNack_Resubmit_Success() {
	executable := s.newTestExecutable()

	s.mockScheduler.EXPECT().TrySubmit(executable).DoAndReturn(func(_ queues.Executable) bool {
		s.Equal(ctasks.TaskStatePending, executable.State())

		go func() {
			// access internal state in a separate goroutine to check if there's any race condition
			// between reschdule and nack.
			s.accessInternalState(executable)
		}()
		return true
	})

	executable.Nack(errors.New("some random error"))
}

func (s *executableSuite) TestTaskNack_Resubmit_Fail() {
	executable := s.newTestExecutable()

	s.mockScheduler.EXPECT().TrySubmit(executable).Return(false)
	s.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now())).Do(func(_ queues.Executable, _ time.Time) {
		s.Equal(ctasks.TaskStatePending, executable.State())

		go func() {
			// access internal state in a separate goroutine to check if there's any race condition
			// between reschdule and nack.
			s.accessInternalState(executable)
		}()
	}).Times(1)

	executable.Nack(errors.New("some random error"))
}

func (s *executableSuite) TestTaskNack_Reschedule() {

	testCases := []struct {
		name    string
		taskErr error
	}{
		{
			name:    "ErrTaskRetry",
			taskErr: consts.ErrTaskRetry, // this error won't trigger re-submit
		},
		{
			name:    "ErrDeleteOpenExecErr",
			taskErr: consts.ErrDependencyTaskNotCompleted, // this error won't trigger re-submit
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			executable := s.newTestExecutable()

			s.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now())).Do(func(_ queues.Executable, _ time.Time) {
				s.Equal(ctasks.TaskStatePending, executable.State())

				go func() {
					// access internal state in a separate goroutine to check if there's any race condition
					// between reschdule and nack.
					s.accessInternalState(executable)
				}()
			}).Times(1)

			executable.Nack(tc.taskErr)
		})
	}
}

func (s *executableSuite) TestTaskAbort() {
	executable := s.newTestExecutable()

	executable.Abort()

	s.NoError(executable.Execute()) // should be no-op and won't invoke executor

	executable.Ack() // should be no-op
	s.Equal(ctasks.TaskStateAborted, executable.State())

	executable.Nack(errors.New("some random error")) // should be no-op and won't invoke scheduler or rescheduler

	executable.Reschedule() // should be no-op and won't invoke rescheduler

	// all error should be treated as non-retryable to break retry loop
	s.False(executable.IsRetryableError(errors.New("some random error")))
}

func (s *executableSuite) TestTaskCancellation() {
	executable := s.newTestExecutable()

	executable.Cancel()

	s.NoError(executable.Execute()) // should be no-op and won't invoke executor

	executable.Ack() // should be no-op
	s.Equal(ctasks.TaskStateCancelled, executable.State())

	executable.Nack(errors.New("some random error")) // should be no-op and won't invoke scheduler or rescheduler

	executable.Reschedule() // should be no-op and won't invoke rescheduler

	// all error should be treated as non-retryable to break retry loop
	s.False(executable.IsRetryableError(errors.New("some random error")))
}

func (s *executableSuite) TestExecute_SendToDLQErrPatternDoesNotMatch() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "testpattern"
		}
	})
	executionError := errors.New("some random error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	s.Error(err2)
	s.NotErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), executionError.Error())

	// Attempt 2
	s.Error(executable.Execute())
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendToDLQErrPatternEmptyString() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return ""
		}
	})
	executionError := errors.New("some random error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	s.Error(err2)
	s.NotErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), executionError.Error())

	// Attempt 2
	s.Error(executable.Execute())
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_SendToDLQErrPatternMatchesMultiple() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable1 := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "test substring 1|test substring 2"
		}
	})
	executionError1 := errors.New("some random error with test substring 1")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable1).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError1,
	}).Times(1)

	executable2 := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return true
		}
		p.dlqErrorPattern = func() string {
			return "test substring 1|test substring 2"
		}
	})
	executionError2 := errors.New("some random error with test substring 2")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable2).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError2,
	}).Times(1)

	// Attempt 1
	err := executable1.Execute()
	err2 := executable1.HandleErr(err)
	s.Error(err2)
	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), executionError1.Error())

	err = executable2.Execute()
	err2 = executable2.HandleErr(err)
	s.Error(err2)
	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), executionError2.Error())

	// Attempt 2
	s.NoError(executable1.Execute())
	s.NoError(executable2.Execute())
	s.Len(queueWriter.EnqueueTaskRequests, 2)
}

func (s *executableSuite) TestExecute_ErrPatternIfDLQDisabled() {
	queueWriter := &queuestest.FakeQueueWriter{}
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return false
		}
		p.dlqErrorPattern = func() string {
			return "test substring"
		}
	})
	executionError := errors.New("some random error with test substring")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        executionError,
	}).Times(2)

	// Attempt 1
	err := executable.Execute()
	err2 := executable.HandleErr(err)
	s.Error(err2)
	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)
	s.Contains(err2.Error(), executionError.Error())

	// Attempt 2
	s.Error(executable.Execute())
	s.Error(executable.HandleErr(err))
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) TestExecute_ErrorErrPatternThenDisableDLQ() {
	queueWriter := &queuestest.FakeQueueWriter{}
	dlqEnabled := true
	executable := s.newTestExecutable(func(p *params) {
		p.dlqWriter = queues.NewDLQWriter(queueWriter, s.mockClusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry)
		p.dlqEnabled = func() bool {
			return dlqEnabled
		}
		p.dlqErrorPattern = func() string {
			return "test substring"
		}
	})
	execError := errors.New("some random error with test substring")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError,
	}).Times(1)

	err := executable.Execute()
	err2 := executable.HandleErr(err)

	s.ErrorIs(err2, queues.ErrTerminalTaskFailure)

	dlqEnabled = false

	// Make sure task is retried this time
	execError2 := errors.New("some other random error")
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
		ExecutionMetricTags: nil,
		ExecutedAsActive:    false,
		ExecutionErr:        execError2,
	}).Times(1)
	s.ErrorIs(executable.Execute(), execError2)
	s.Empty(queueWriter.EnqueueTaskRequests)
}

func (s *executableSuite) newTestExecutable(opts ...option) queues.Executable {
	p := params{
		dlqWriter: nil,
		dlqEnabled: func() bool {
			return false
		},
		dlqInternalErrors: func() bool {
			return false
		},
		priorityAssigner: queues.NewNoopPriorityAssigner(),
		maxUnexpectedErrorAttempts: func() int {
			return math.MaxInt
		},
		dlqErrorPattern: func() string {
			return ""
		},
	}
	for _, opt := range opts {
		opt(&p)
	}
	return queues.NewExecutable(
		queues.DefaultReaderId,
		tasks.NewFakeTask(
			definition.NewWorkflowKey(
				tests.NamespaceID.String(),
				tests.WorkflowID,
				tests.RunID,
			),
			tasks.CategoryTransfer,
			s.timeSource.Now(),
		),
		s.mockExecutor,
		s.mockScheduler,
		s.mockRescheduler,
		p.priorityAssigner,
		s.timeSource,
		s.mockNamespaceRegistry,
		s.mockClusterMetadata,
		log.NewTestLogger(),
		s.metricsHandler,
		func(params *queues.ExecutableParams) {
			params.DLQEnabled = p.dlqEnabled
			params.DLQWriter = p.dlqWriter
			params.MaxUnexpectedErrorAttempts = p.maxUnexpectedErrorAttempts
			params.DLQInternalErrors = p.dlqInternalErrors
			params.DLQErrorPattern = p.dlqErrorPattern
		},
	)
}

func (s *executableSuite) accessInternalState(executable queues.Executable) {
	_ = fmt.Sprintf("%v", executable)
}

// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package workflow_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/plugins/nexusoperations"
	opsworkflow "go.temporal.io/server/plugins/nexusoperations/workflow"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandValidator struct {
	maxPayloadSize int
}

func (v commandValidator) IsValidPayloadSize(size int) bool {
	return size <= v.maxPayloadSize
}

type testContext struct {
	ms              workflow.MutableState
	scheduleHandler workflow.CommandHandler
	cancelHandler   workflow.CommandHandler
	history         *historypb.History
}

var defaultConfig = &nexusoperations.Config{
	Enabled:                 dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
	MaxOperationNameLength:  dynamicconfig.GetIntPropertyFilteredByNamespace(5),
	MaxConcurrentOperations: dynamicconfig.GetIntPropertyFilteredByNamespace(2),
}

func newTestContext(t *testing.T, cfg *nexusoperations.Config) testContext {
	chReg := workflow.NewCommandHandlerRegistry()
	require.NoError(t, opsworkflow.RegisterCommandHandlers(chReg, cfg))
	smReg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smReg))
	require.NoError(t, nexusoperations.RegisterStateMachines(smReg))
	node, err := hsm.NewRoot(smReg, workflow.StateMachineType.ID, nil, make(map[int32]*persistencespb.StateMachineMap))
	require.NoError(t, err)
	ms := workflow.NewMockMutableState(gomock.NewController(t))
	ms.EXPECT().HSM().Return(node).AnyTimes()
	lastEventID := int64(4)
	history := &historypb.History{}
	ms.EXPECT().AddHistoryEvent(gomock.Any(), gomock.Any()).DoAndReturn(func(t enumspb.EventType, setAttrs func(he *historypb.HistoryEvent)) *historypb.HistoryEvent {
		e := &historypb.HistoryEvent{
			Version:   1,
			EventId:   lastEventID,
			EventTime: timestamppb.Now(),
		}
		lastEventID++
		setAttrs(e)
		history.Events = append(history.Events, e)
		return e
	}).AnyTimes()
	ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	scheduleHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION)
	require.True(t, ok)
	cancelHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION)
	require.True(t, ok)

	return testContext{
		ms:              ms,
		history:         history,
		scheduleHandler: scheduleHandler,
		cancelHandler:   cancelHandler,
	}
}

func TestHandleScheduleCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		})
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("exceeds max operation length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Service:   "service",
					Operation: "too long",
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("exceeds max payload size", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Service:   "service",
					Operation: "op",
					Input: &common.Payload{
						Data: []byte("ab"),
					},
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.True(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("exceeds max concurrent operations", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		for i := 0; i < 2; i++ {
			err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   "service",
						Operation: "op",
					},
				},
			})
			require.NoError(t, err)
		}
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Service:   "service",
					Operation: "op",
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_NEXUS_OPERATIONS_LIMIT_EXCEEDED, failWFTErr.Cause)
		require.Equal(t, 2, len(tcx.history.Events))
	})

	timeoutCases := []struct {
		name               string
		workflowRunTimeout *durationpb.Duration
		commandTimeout     *durationpb.Duration
		expectedTimeout    *durationpb.Duration
	}{
		{
			name:               "operation timeout defaults to workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     nil,
			expectedTimeout:    durationpb.New(time.Minute),
		},
		{
			name:               "operation timeout trimmed to workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     durationpb.New(time.Hour),
			expectedTimeout:    durationpb.New(time.Minute),
		},
		{
			name:               "operation timeout left as is if less than workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     durationpb.New(time.Second),
			expectedTimeout:    durationpb.New(time.Second),
		},
		{
			name:               "operation timeout left as is if no workflow run timeout",
			workflowRunTimeout: nil,
			commandTimeout:     durationpb.New(time.Second),
			expectedTimeout:    durationpb.New(time.Second),
		},
	}
	for _, tc := range timeoutCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)

			tcx.ms.GetExecutionInfo().WorkflowRunTimeout = tc.workflowRunTimeout
			err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   "service",
						Operation: "op",
						Timeout:   tc.commandTimeout,
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(tcx.history.Events))
			require.Equal(t, tc.expectedTimeout.AsDuration(), tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().Timeout.AsDuration())
		})
	}

	t.Run("sets event attributes and spawns a child operation machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		cAttrs := &commandpb.ScheduleNexusOperationCommandAttributes{
			Service:   "service",
			Operation: "op",
			Input:     &common.Payload{},
			Header: map[string]string{
				"key": "value",
			},
			Timeout: durationpb.New(time.Hour),
		}
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: cAttrs,
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		event := tcx.history.Events[0]
		eAttrs := event.GetNexusOperationScheduledEventAttributes()
		require.Equal(t, cAttrs.Service, eAttrs.Service)
		require.Equal(t, cAttrs.Operation, eAttrs.Operation)
		require.Equal(t, cAttrs.Input, eAttrs.Input)
		require.Equal(t, cAttrs.Timeout, eAttrs.Timeout)
		require.Equal(t, cAttrs.Header, eAttrs.Header)
		require.Equal(t, int64(1), eAttrs.WorkflowTaskCompletedEventId)
		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType.ID, ID: strconv.FormatInt(event.EventId, 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)
	})

}

func TestHandleCancelCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		})
		err := tcx.cancelHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("operation not found", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: 5,
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("sets event attributes and spawns cancelation child machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Service:   "service",
					Operation: "op",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		event := tcx.history.Events[0]

		err = tcx.cancelHandler(tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		})
		require.NoError(t, err)

		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType.ID, ID: strconv.FormatInt(event.EventId, 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)

		require.Equal(t, 2, len(tcx.history.Events))
		attrs := tcx.history.Events[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.EventId, attrs.ScheduledEventId)
		require.Equal(t, int64(1), attrs.WorkflowTaskCompletedEventId)

		child, err = child.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
		require.NoError(t, err)
		require.NotNil(t, child)
	})
}

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
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/components/nexusoperations"
	opsworkflow "go.temporal.io/server/components/nexusoperations/workflow"
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
	execInfo        *persistencespb.WorkflowExecutionInfo
	ms              workflow.MutableState
	scheduleHandler workflow.CommandHandler
	cancelHandler   workflow.CommandHandler
	history         *historypb.History
}

var defaultConfig = &nexusoperations.Config{
	Enabled:                            dynamicconfig.GetBoolPropertyFn(true),
	MaxServiceNameLength:               dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("service")),
	MaxOperationNameLength:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("op")),
	MaxConcurrentOperations:            dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
	MaxOperationScheduleToCloseTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour * 24),
}

func newTestContext(t *testing.T, cfg *nexusoperations.Config) testContext {
	endpointReg := nexustest.FakeEndpointRegistry{
		OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
			if endpointName != "endpoint" {
				return nil, serviceerror.NewNotFound("endpoint not found")
			}
			// Only the ID is taken here.
			return &persistencespb.NexusEndpointEntry{Id: "endpoint-id"}, nil
		},
	}
	chReg := workflow.NewCommandHandlerRegistry()
	require.NoError(t, opsworkflow.RegisterCommandHandlers(chReg, endpointReg, cfg))
	smReg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smReg))
	require.NoError(t, nexusoperations.RegisterStateMachines(smReg))
	require.NoError(t, nexusoperations.RegisterEventDefinitions(smReg))
	ms := workflow.NewMockMutableState(gomock.NewController(t))
	node, err := hsm.NewRoot(smReg, workflow.StateMachineType.ID, ms, make(map[int32]*persistencespb.StateMachineMap), ms)
	require.NoError(t, err)
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

	execInfo := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().TransitionCount().Return(int64(1)).AnyTimes()
	scheduleHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION)
	require.True(t, ok)
	cancelHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION)
	require.True(t, ok)

	return testContext{
		execInfo:        execInfo,
		ms:              ms,
		history:         history,
		scheduleHandler: scheduleHandler,
		cancelHandler:   cancelHandler,
	}
}

func TestHandleScheduleCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("endpoint not found", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "not found",
					Service:   "service",
					Operation: "op",
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("exceeds max service length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "too long",
					Operation: "op",
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("exceeds max operation length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
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
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
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
			err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  "endpoint",
						Service:   "service",
						Operation: "op",
					},
				},
			})
			require.NoError(t, err)
		}
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
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

	t.Run("schedule to close timeout capped by run timeout", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.execInfo.WorkflowRunTimeout = durationpb.New(time.Hour)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(time.Hour * 2),
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		require.Equal(t, time.Hour, tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("schedule to close timeout capped by dynamic config", func(t *testing.T) {
		cfg := *defaultConfig
		cfg.MaxOperationScheduleToCloseTimeout = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute)
		tcx := newTestContext(t, &cfg)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		require.Equal(t, time.Minute, tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().ScheduleToCloseTimeout.AsDuration())
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
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)

			tcx.ms.GetExecutionInfo().WorkflowRunTimeout = tc.workflowRunTimeout
			err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:               "endpoint",
						Service:                "service",
						Operation:              "op",
						ScheduleToCloseTimeout: tc.commandTimeout,
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(tcx.history.Events))
			require.Equal(t, tc.expectedTimeout.AsDuration(), tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().ScheduleToCloseTimeout.AsDuration())
		})
	}

	t.Run("sets event attributes with UserMetadata and spawns a child operation machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		cAttrs := &commandpb.ScheduleNexusOperationCommandAttributes{
			Endpoint:  "endpoint",
			Service:   "service",
			Operation: "op",
			Input:     &common.Payload{},
			NexusHeader: map[string]string{
				"key": "value",
			},
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
		}
		userMetadata := &sdkpb.UserMetadata{
			Summary: &common.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &common.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: cAttrs,
			},
			UserMetadata: userMetadata,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		event := tcx.history.Events[0]
		eAttrs := event.GetNexusOperationScheduledEventAttributes()
		require.Equal(t, cAttrs.Service, eAttrs.Service)
		require.Equal(t, cAttrs.Operation, eAttrs.Operation)
		require.Equal(t, cAttrs.Input, eAttrs.Input)
		require.Equal(t, cAttrs.ScheduleToCloseTimeout, eAttrs.ScheduleToCloseTimeout)
		require.Equal(t, cAttrs.NexusHeader, eAttrs.NexusHeader)
		require.Equal(t, int64(1), eAttrs.WorkflowTaskCompletedEventId)
		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType.ID, ID: strconv.FormatInt(event.EventId, 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)
		require.EqualExportedValues(t, userMetadata, event.UserMetadata)
	})

}

func TestHandleCancelCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.Events))
	})

	t.Run("operation not found", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
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

	t.Run("operation already completed", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		event := tcx.history.Events[0]

		coll := nexusoperations.MachineCollection(tcx.ms.HSM())
		node, err := coll.Node(strconv.FormatInt(event.EventId, 10))
		require.NoError(t, err)
		op, err := coll.Data(strconv.FormatInt(event.EventId, 10))
		require.NoError(t, err)
		_, err = nexusoperations.TransitionSucceeded.Apply(op, nexusoperations.EventSucceeded{
			Node: node,
		})
		require.NoError(t, err)

		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.FailWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 1, len(tcx.history.Events)) // Only scheduled event should be recorded.
	})

	t.Run("sets event attributes with UserMetadata and spawns cancelation child machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		})
		userMetadata := &sdkpb.UserMetadata{
			Summary: &common.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &common.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.Events))
		event := tcx.history.Events[0]

		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
			UserMetadata: userMetadata,
		})
		require.NoError(t, err)

		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType.ID, ID: strconv.FormatInt(event.EventId, 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)

		require.Equal(t, 3, len(tcx.history.Events))
		crAttrs := tcx.history.Events[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.EventId, crAttrs.ScheduledEventId)
		require.Equal(t, int64(1), crAttrs.WorkflowTaskCompletedEventId)
		savedUserMetadata := tcx.history.Events[1].GetUserMetadata()
		require.EqualExportedValues(t, userMetadata, savedUserMetadata)

		cAttrs := tcx.history.Events[2].GetNexusOperationCanceledEventAttributes()
		require.Equal(t, event.EventId, cAttrs.ScheduledEventId)
		require.Equal(t, "operation canceled before started", cAttrs.Failure.Cause.Message)
		require.NotNil(t, cAttrs.Failure.Cause.GetCanceledFailureInfo())

		child, err = child.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
		require.NoError(t, err)
		require.NotNil(t, child)
		userMetadata = nil
	})
}

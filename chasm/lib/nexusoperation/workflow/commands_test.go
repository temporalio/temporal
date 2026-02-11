package workflow

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/chasm/lib/workflow/command"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandValidator struct {
	maxPayloadSize int
}

func (v commandValidator) IsValidPayloadSize(size int) bool {
	return size <= v.maxPayloadSize
}

func operationKey(eventID int64) string {
	return strconv.FormatInt(eventID, 10)
}

type testContext struct {
	chasmCtx        *chasm.MockMutableContext
	wf              *chasmworkflow.Workflow
	execInfo        *persistencespb.WorkflowExecutionInfo
	scheduleHandler command.Handler
	cancelHandler   command.Handler
	history         *historypb.History
}

var defaultConfig = &nexusoperation.Config{
	Enabled:                            dynamicconfig.GetBoolPropertyFn(true),
	MaxServiceNameLength:               dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("service")),
	MaxOperationNameLength:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("op")),
	MaxConcurrentOperations:            dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
	MaxOperationHeaderSize:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(20),
	DisallowedOperationHeaders:         dynamicconfig.GetTypedPropertyFn([]string{"request-timeout"}),
	MaxOperationScheduleToCloseTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour * 24),
}

func newTestContext(t *testing.T, cfg *nexusoperation.Config) testContext {
	endpointReg := nexustest.FakeEndpointRegistry{
		OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
			if endpointName == "endpoint caller namespace unauthorized" {
				return nil, serviceerror.NewPermissionDenied("caller namespace unauthorized", "")
			} else if endpointName != "endpoint" {
				return nil, serviceerror.NewNotFound("endpoint not found")
			}
			// Only the ID is taken here.
			return &persistencespb.NexusEndpointEntry{Id: "endpoint-id"}, nil
		},
	}
	chReg := command.NewRegistry()
	require.NoError(t, registerCommandHandlers(chReg, cfg, endpointReg))

	execInfo := &persistencespb.WorkflowExecutionInfo{}
	backend := &chasm.MockNodeBackend{
		HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo {
			return execInfo
		},
		HandleGetNamespaceEntry: func() *namespace.Namespace {
			return tests.GlobalNamespaceEntry
		},
	}

	lastEventID := int64(4)
	history := &historypb.History{}
	backend.HandleAddHistoryEvent = func(t enumspb.EventType, setAttrs func(he *historypb.HistoryEvent)) *historypb.HistoryEvent {
		e := &historypb.HistoryEvent{
			Version:   1,
			EventId:   lastEventID,
			EventTime: timestamppb.Now(),
		}
		lastEventID++
		setAttrs(e)
		history.Events = append(history.Events, e)
		return e
	}

	chasmCtx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleGetNamespaceEntry: func() *namespace.Namespace {
				return tests.GlobalNamespaceEntry
			},
		},
		HandleAddHistoryEvent: backend.HandleAddHistoryEvent,
	}

	wf := &chasmworkflow.Workflow{
		MSPointer: chasm.NewMSPointer(backend),
	}

	scheduleHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION)
	require.True(t, ok)
	cancelHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION)
	require.True(t, ok)

	return testContext{
		chasmCtx:        chasmCtx,
		wf:              wf,
		execInfo:        execInfo,
		history:         history,
		scheduleHandler: scheduleHandler,
		cancelHandler:   cancelHandler,
	}
}

func TestHandleScheduleCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperation.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("endpoint not found - rejected by config", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "not found",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("caller namespace unauthorized", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint caller namespace unauthorized",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("exceeds max service length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "too long",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("exceeds max operation length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "too long",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("exceeds max operation header size", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
					NexusHeader: map[string]string{
						"key1234567890": "value1234567890",
					},
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("invalid header keys", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
					NexusHeader: map[string]string{
						"request-timeout": "1s",
					},
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("exceeds max payload size", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
					Input: &commonpb.Payload{
						Data: []byte("ab"),
					},
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.True(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("exceeds max concurrent operations", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		for i := 0; i < 2; i++ {
			err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  "endpoint",
						Service:   "service",
						Operation: "op",
					},
				},
			}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
			require.NoError(t, err)
		}
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_NEXUS_OPERATIONS_LIMIT_EXCEEDED, failWFTErr.Cause)
		require.Len(t, tcx.history.Events, 2)
	})

	t.Run("schedule to close timeout capped by run timeout", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		tcx.execInfo.WorkflowRunTimeout = durationpb.New(time.Hour)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(time.Hour * 2),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		require.Equal(t, time.Hour, tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("schedule to close timeout capped by dynamic config", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		cfg := *defaultConfig
		cfg.MaxOperationScheduleToCloseTimeout = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute)
		tcx := newTestContext(t, &cfg)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
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
			t.Skip("requires TransitionScheduled implementation")
			tcx := newTestContext(t, defaultConfig)

			tcx.execInfo.WorkflowRunTimeout = tc.workflowRunTimeout
			err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:               "endpoint",
						Service:                "service",
						Operation:              "op",
						ScheduleToCloseTimeout: tc.commandTimeout,
					},
				},
			}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
			require.NoError(t, err)
			require.Len(t, tcx.history.Events, 1)
			require.Equal(t, tc.expectedTimeout.AsDuration(), tcx.history.Events[0].GetNexusOperationScheduledEventAttributes().ScheduleToCloseTimeout.AsDuration())
		})
	}

	t.Run("invalid schedule-to-start timeout", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToStartTimeout: durationpb.New(-1 * time.Second),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("invalid start-to-close timeout", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:            "endpoint",
					Service:             "service",
					Operation:           "op",
					StartToCloseTimeout: durationpb.New(-1 * time.Second),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("schedule-to-start timeout trimmed to schedule-to-close timeout", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(time.Hour),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		eAttrs := tcx.history.Events[0].GetNexusOperationScheduledEventAttributes()
		require.Equal(t, 30*time.Minute, eAttrs.ScheduleToStartTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, eAttrs.ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("start-to-close timeout trimmed to schedule-to-close timeout", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
					StartToCloseTimeout:    durationpb.New(time.Hour),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		eAttrs := tcx.history.Events[0].GetNexusOperationScheduledEventAttributes()
		require.Equal(t, 30*time.Minute, eAttrs.StartToCloseTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, eAttrs.ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("both secondary timeouts trimmed to schedule-to-close timeout", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(time.Hour),
					StartToCloseTimeout:    durationpb.New(2 * time.Hour),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		eAttrs := tcx.history.Events[0].GetNexusOperationScheduledEventAttributes()
		require.Equal(t, 30*time.Minute, eAttrs.ScheduleToStartTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, eAttrs.StartToCloseTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, eAttrs.ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("secondary timeouts not trimmed when less than schedule-to-close timeout", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					ScheduleToStartTimeout: durationpb.New(20 * time.Minute),
					StartToCloseTimeout:    durationpb.New(30 * time.Minute),
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		eAttrs := tcx.history.Events[0].GetNexusOperationScheduledEventAttributes()
		require.Equal(t, 20*time.Minute, eAttrs.ScheduleToStartTimeout.AsDuration())
		require.Equal(t, 30*time.Minute, eAttrs.StartToCloseTimeout.AsDuration())
		require.Equal(t, time.Hour, eAttrs.ScheduleToCloseTimeout.AsDuration())
	})

	t.Run("sets event attributes with UserMetadata and creates an operation component", func(t *testing.T) {
		t.Skip("requires TransitionScheduled implementation")
		tcx := newTestContext(t, defaultConfig)
		cAttrs := &commandpb.ScheduleNexusOperationCommandAttributes{
			Endpoint:  "endpoint",
			Service:   "service",
			Operation: "op",
			Input:     &commonpb.Payload{},
			NexusHeader: map[string]string{
				"key": "value",
			},
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
		}
		userMetadata := &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: cAttrs,
			},
			UserMetadata: userMetadata,
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)

		event := tcx.history.Events[0]
		eAttrs := event.GetNexusOperationScheduledEventAttributes()
		require.Equal(t, cAttrs.Service, eAttrs.Service)
		require.Equal(t, cAttrs.Operation, eAttrs.Operation)
		require.Equal(t, cAttrs.Input, eAttrs.Input)
		require.Equal(t, cAttrs.ScheduleToCloseTimeout, eAttrs.ScheduleToCloseTimeout)
		require.Equal(t, cAttrs.NexusHeader, eAttrs.NexusHeader)
		require.Equal(t, int64(1), eAttrs.WorkflowTaskCompletedEventId)

		key := operationKey(event.EventId)
		opField, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := opField.Get(tcx.chasmCtx)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)

		ref := &tokenspb.HistoryEventRef{}
		require.NoError(t, proto.Unmarshal(op.ScheduledEventToken, ref))
		require.Equal(t, event.EventId, ref.EventId)
		require.Equal(t, int64(1), ref.EventBatchId) // WorkflowTaskCompletedEventID
		require.EqualExportedValues(t, userMetadata, event.UserMetadata)
	})
}

func TestHandleCancelCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation implementation")
		tcx := newTestContext(t, &nexusoperation.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("empty attributes", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("operation not found", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation implementation")
		tcx := newTestContext(t, defaultConfig)

		err := tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: 5,
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Empty(t, tcx.history.Events)
	})

	t.Run("operation already completed", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation implementation")
		tcx := newTestContext(t, defaultConfig)

		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		event := tcx.history.Events[0]

		// TODO: Complete the operation using CHASM equivalent of CompletedEventDefinition.

		// Try to cancel - should fail since operation is completed/deleted.
		err = tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Len(t, tcx.history.Events, 1) // Only scheduled event should be recorded.
	})

	t.Run("operation already completed - completion buffered", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation and buffered event support")
		tcx := newTestContext(t, defaultConfig)
		// TODO: CHASM equivalent of HasAnyBufferedEvent setup.

		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		event := tcx.history.Events[0]

		// TODO: Complete the operation using CHASM equivalent of CompletedEventDefinition.

		// Try to cancel - should succeed because there's a buffered completion.
		err = tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 2) // Both scheduled and cancel requested events should be recorded.
		crAttrs := tcx.history.Events[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.EventId, crAttrs.ScheduledEventId)
	})

	t.Run("sets event attributes with UserMetadata and spawns cancelation child machine", func(t *testing.T) {
		t.Skip("requires CHASM nexus operation cancellation implementation")
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		userMetadata := &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		event := tcx.history.Events[0]

		err = tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: event.EventId,
				},
			},
			UserMetadata: userMetadata,
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)

		key := operationKey(event.EventId)
		_, ok := tcx.wf.Operations[key]
		require.True(t, ok)

		require.Len(t, tcx.history.Events, 2)
		crAttrs := tcx.history.Events[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.EventId, crAttrs.ScheduledEventId)
		require.Equal(t, int64(1), crAttrs.WorkflowTaskCompletedEventId)
		savedUserMetadata := tcx.history.Events[1].GetUserMetadata()
		require.EqualExportedValues(t, userMetadata, savedUserMetadata)

		// TODO: Verify cancelation child component exists (CHASM equivalent of HSM CancelationMachineKey check).
	})
}

func TestOperationNodeDeletionOnTerminalEvents(t *testing.T) {
	t.Skip("requires CHASM operation lifecycle implementation")

	scheduleOperation := func(t *testing.T, tcx testContext) (scheduledEvent *historypb.HistoryEvent, nodeID string) {
		err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 100}, &commandpb.Command{
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 1})
		require.NoError(t, err)
		require.Len(t, tcx.history.Events, 1)
		scheduledEvent = tcx.history.Events[0]

		nodeID = operationKey(scheduledEvent.EventId)
		_, ok := tcx.wf.Operations[nodeID]
		require.True(t, ok)
		return
	}

	applyTerminalEventAndAssertDeletion := func(
		t *testing.T,
		tcx testContext,
		scheduledEventID int64,
		eventType enumspb.EventType,
		eventAttr interface{},
	) {
		nodeID := operationKey(scheduledEventID)

		event := &historypb.HistoryEvent{
			Version:   1,
			EventId:   scheduledEventID + 1,
			EventType: eventType,
			EventTime: timestamppb.Now(),
		}

		switch eventType { //nolint:exhaustive
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			event.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: eventAttr.(*historypb.NexusOperationCompletedEventAttributes),
			}
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			event.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: eventAttr.(*historypb.NexusOperationFailedEventAttributes),
			}
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
			event.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: eventAttr.(*historypb.NexusOperationCanceledEventAttributes),
			}
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			event.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: eventAttr.(*historypb.NexusOperationTimedOutEventAttributes),
			}
		default:
			panic(fmt.Sprintf("unexpected event type in test: %v", eventType))
		}

		// TODO: Apply the terminal event using CHASM equivalent of HSM EventDefinition.
		_ = event

		_, ok := tcx.wf.Operations[nodeID]
		require.False(t, ok, "operation should be deleted after terminal event")

		err := tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
			Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
				RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
					ScheduledEventId: scheduledEventID,
				},
			},
		}, command.HandlerOptions{WorkflowTaskCompletedEventID: 2})
		var failWFTErr command.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Len(t, tcx.history.Events, 1, "no new events after attempting to cancel a terminated operation")
	}

	cases := []struct {
		name      string
		eventType enumspb.EventType
		eventAttr interface{}
	}{
		{
			name:      "completed event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
			eventAttr: &historypb.NexusOperationCompletedEventAttributes{},
		},
		{
			name:      "failed event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
			eventAttr: &historypb.NexusOperationFailedEventAttributes{},
		},
		{
			name:      "canceled event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
			eventAttr: &historypb.NexusOperationCanceledEventAttributes{},
		},
		{
			name:      "timed out event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
			eventAttr: &historypb.NexusOperationTimedOutEventAttributes{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)
			scheduledEvent, _ := scheduleOperation(t, tcx)

			switch a := tc.eventAttr.(type) {
			case *historypb.NexusOperationCompletedEventAttributes:
				a.ScheduledEventId = scheduledEvent.EventId
			case *historypb.NexusOperationFailedEventAttributes:
				a.ScheduledEventId = scheduledEvent.EventId
			case *historypb.NexusOperationCanceledEventAttributes:
				a.ScheduledEventId = scheduledEvent.EventId
			case *historypb.NexusOperationTimedOutEventAttributes:
				a.ScheduledEventId = scheduledEvent.EventId
			}

			applyTerminalEventAndAssertDeletion(t, tcx, scheduledEvent.EventId, tc.eventType, tc.eventAttr)
		})
	}
}

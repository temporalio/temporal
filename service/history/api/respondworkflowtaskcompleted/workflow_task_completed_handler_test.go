package respondworkflowtaskcompleted

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsregistry"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestCommandProtocolMessage(t *testing.T) {
	t.Parallel()

	type testconf struct {
		ms      *historyi.MockMutableState
		updates update.Registry
		handler *workflowTaskCompletedHandler
	}

	const defaultBlobSizeLimit = 1 * 1024 * 1024

	msgCommand := func(msgID string) *commandpb.Command {
		return &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{
				ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
					MessageId: msgID,
				},
			},
		}
	}

	setup := func(t *testing.T, out *testconf, blobSizeLimit int) {
		shardCtx := historyi.NewMockShardContext(gomock.NewController(t))
		logger := log.NewNoopLogger()
		metricsHandler := metrics.NoopMetricsHandler
		out.ms = historyi.NewMockMutableState(gomock.NewController(t))
		out.ms.EXPECT().VisitUpdates(gomock.Any())
		out.ms.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
		out.ms.EXPECT().GetCurrentVersion().Return(tests.LocalNamespaceEntry.FailoverVersion())

		out.updates = update.NewRegistry(out.ms)
		var effects effect.Buffer
		col := dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), logger)
		config := configs.NewConfig(col, 1)
		mockMeta := persistence.NewMockMetadataManager(gomock.NewController(t))
		nsReg := nsregistry.NewRegistry(
			mockMeta,
			true,
			func() time.Duration { return 1 * time.Hour },
			dynamicconfig.GetBoolPropertyFn(false),
			metricsHandler,
			logger,
			namespace.NewDefaultReplicationResolverFactory(),
		)
		out.handler = newWorkflowTaskCompletedHandler( // 😲
			t.Name(), // identity
			123,      // workflowTaskCompletedID
			out.ms,
			out.updates,
			&effects,
			api.NewCommandAttrValidator(
				nsReg,
				config,
				nil, // searchAttributesValidator
			),
			newWorkflowSizeChecker(
				workflowSizeLimits{blobSizeLimitError: blobSizeLimit},
				out.ms,
				nil, // searchAttributesValidator
				metricsHandler,
				logger,
			),
			logger,
			nsReg,
			metricsHandler,
			config,
			shardCtx,
			nil, // searchattribute.MapperProvider
			false,
			nil, // TODO: test usage of commandHandlerRegistry?
			nil,
		)
	}

	t.Run("missing message ID", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			command = msgCommand("") // blank is invalid
		)

		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList())
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
	})

	// Verifies that if user metadata is present in the command then it is passed through to the newly created event.
	t.Run("Attach user metadata", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		msgID := t.Name() + "-message-id"
		command := msgCommand(msgID)
		startTimerCommandAttributes := &commandpb.StartTimerCommandAttributes{
			TimerId: fmt.Sprintf("random-timer-id-%d", rand.Int63()),
		}
		command.UserMetadata = &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &commonpb.Payload{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}

		command.CommandType = enumspb.COMMAND_TYPE_START_TIMER
		command.Attributes = &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: startTimerCommandAttributes,
		}

		// mock an event creation.
		event := &historypb.HistoryEvent{}
		tc.ms.EXPECT().AddTimerStartedEvent(tc.handler.workflowTaskCompletedID, startTimerCommandAttributes).MaxTimes(1).Return(event, nil, nil)

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList())
		require.NoError(t, err)

		// Verify that the user metadata is populated properly in the event object
		require.Equal(t, event.UserMetadata, command.UserMetadata)
	})

	// Verifies that the error is properly handled when event creation fails.
	t.Run("Event creation failure", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		command := msgCommand(t.Name() + "-message-id")
		completeWorkflowExecutionCommandAttributes := &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{},
			},
		}
		command.UserMetadata = &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{},
			Details: &commonpb.Payload{},
		}

		command.CommandType = enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
		command.Attributes = &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: completeWorkflowExecutionCommandAttributes,
		}

		// mock a failed event creation.
		event := &historypb.HistoryEvent{}
		tc.ms.EXPECT().AddCompletedWorkflowEvent(tc.handler.workflowTaskCompletedID, completeWorkflowExecutionCommandAttributes, "").MaxTimes(1).Return(event, fmt.Errorf("FAIL"))
		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})
		tc.ms.EXPECT().IsWorkflowExecutionRunning().AnyTimes().Return(true)
		tc.ms.EXPECT().GetCronBackoffDuration().AnyTimes().Return(backoff.NoBackoff)

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList())
		require.Error(t, err)

		// Verify that the event is discarded anduser metadata is not attached to the event.
		require.Nil(t, event.UserMetadata)
	})

	t.Run("message not found", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			command = msgCommand("valid_but_not_found_msg_id")
		)

		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList())
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
	})

	t.Run("message too large", func(t *testing.T) {
		var tc testconf
		t.Log("setting max blob size to zero")
		setup(t, &tc, 0)
		var (
			msgID   = t.Name() + "-message-id"
			command = msgCommand(msgID) // blank is invalid
			msg     = &protocolpb.Message{
				Id:                 msgID,
				ProtocolInstanceId: "does_not_matter",
				Body:               mustMarshalAny(t, &anypb.Any{}),
			}
		)

		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList(msg))
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
		require.ErrorContains(t, tc.handler.workflowTaskFailedCause.causeErr, "exceeds size limit")
	})

	t.Run("message for unsupported protocol", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			msgID   = t.Name() + "-message-id"
			command = msgCommand(msgID) // blank is invalid
			msg     = &protocolpb.Message{
				Id:                 msgID,
				ProtocolInstanceId: "does_not_matter",
				Body:               mustMarshalAny(t, &anypb.Any{}),
			}
		)

		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList(msg))
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, tc.handler.workflowTaskFailedCause.causeErr, &invalidArg)
		require.ErrorContains(t, tc.handler.workflowTaskFailedCause.causeErr, "protocol type")
	})

	t.Run("update not found", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			msgID   = t.Name() + "-message-id"
			command = msgCommand(msgID) // blank is invalid
			msg     = &protocolpb.Message{
				Id:                 msgID,
				ProtocolInstanceId: "will not be found",
				Body:               mustMarshalAny(t, &updatepb.Acceptance{}),
			}
		)

		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})
		tc.ms.EXPECT().GetUpdateOutcome(gomock.Any(), "will not be found").Return(nil, serviceerror.NewNotFound(""))

		_, err := tc.handler.handleCommand(context.Background(), command, newMsgList(msg))
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
		var notfound *serviceerror.NotFound
		require.ErrorAs(t, tc.handler.workflowTaskFailedCause.causeErr, &notfound)
	})

	t.Run("deliver message failure", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			updateID = t.Name() + "-update-id"
			msgID    = t.Name() + "-message-id"
			command  = msgCommand(msgID) // blank is invalid
			msg      = &protocolpb.Message{
				Id:                 msgID,
				ProtocolInstanceId: updateID,
				Body:               mustMarshalAny(t, &updatepb.Acceptance{}),
			}
		)
		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})
		tc.ms.EXPECT().GetUpdateOutcome(gomock.Any(), updateID).Return(nil, serviceerror.NewNotFound(""))
		tc.ms.EXPECT().IsWorkflowExecutionRunning().AnyTimes().Return(true)

		t.Log("create the expected protocol instance")
		_, _, err := tc.updates.FindOrCreate(context.Background(), updateID)
		require.NoError(t, err)

		t.Log("delivering an acceptance message to an update in the admitted state should cause a protocol error")
		_, err = tc.handler.handleCommand(context.Background(), command, newMsgList(msg))
		require.NoError(t, err)
		require.NotNil(t, tc.handler.workflowTaskFailedCause)
		require.Equal(t,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			tc.handler.workflowTaskFailedCause.failedCause)
		var gotErr *serviceerror.InvalidArgument
		require.ErrorAs(t, tc.handler.workflowTaskFailedCause.causeErr, &gotErr)
	})

	t.Run("deliver message success", func(t *testing.T) {
		var tc testconf
		setup(t, &tc, defaultBlobSizeLimit)
		var (
			updateID = t.Name() + "-update-id"
			msgID    = updateID + "/request"
			command  = msgCommand(msgID) // blank is invalid
			req      = &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{Name: "not_empty"},
			}
			msg = &protocolpb.Message{
				Id:                 msgID,
				ProtocolInstanceId: updateID,
				Body: mustMarshalAny(t, &updatepb.Acceptance{
					AcceptedRequestMessageId:         msgID,
					AcceptedRequestSequencingEventId: 2208,
					AcceptedRequest:                  req,
				}),
			}
			msgs = newMsgList(msg)
		)
		tc.ms.EXPECT().GetExecutionInfo().AnyTimes().Return(&persistencespb.WorkflowExecutionInfo{})
		tc.ms.EXPECT().GetExecutionState().AnyTimes().Return(&persistencespb.WorkflowExecutionState{})
		tc.ms.EXPECT().GetUpdateOutcome(gomock.Any(), updateID).Return(nil, serviceerror.NewNotFound(""))
		tc.ms.EXPECT().IsWorkflowExecutionRunning().AnyTimes().Return(true)
		tc.ms.EXPECT().AddWorkflowExecutionUpdateAcceptedEvent(updateID, msgID, int64(2208), gomock.Any()).Return(&historypb.HistoryEvent{}, nil)

		t.Log("create the expected protocol instance")
		upd, _, err := tc.updates.FindOrCreate(context.Background(), updateID)
		require.NoError(t, err)
		err = upd.Admit(req, workflow.WithEffects(effect.Immediate(context.Background()), tc.handler.mutableState))
		require.NoError(t, err)
		_ = upd.Send(true, &protocolpb.Message_EventId{EventId: 2208})

		_, err = tc.handler.handleCommand(context.Background(), command, msgs)
		require.NoError(t, err,
			"delivering a acceptance message to an update in the sent state should succeed")
		require.Nil(t, tc.handler.workflowTaskFailedCause)
	})
}

func newMsgList(msgs ...*protocolpb.Message) *collection.IndexedTakeList[string, *protocolpb.Message] {
	return collection.NewIndexedTakeList(msgs, func(msg *protocolpb.Message) string { return msg.Id })
}

func mustMarshalAny(t *testing.T, pb proto.Message) *anypb.Any {
	t.Helper()
	var a anypb.Any
	require.NoError(t, a.MarshalFrom(pb))
	return &a
}

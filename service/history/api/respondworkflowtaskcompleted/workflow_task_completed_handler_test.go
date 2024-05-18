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

package respondworkflowtaskcompleted

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

func TestCommandProtocolMessage(t *testing.T) {
	t.Parallel()

	type testconf struct {
		ms      *workflow.MockMutableState
		updates update.Registry
		handler *workflowTaskCompletedHandler
		conf    map[dynamicconfig.Key]any
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
		shardCtx := shard.NewMockContext(gomock.NewController(t))
		logger := log.NewNoopLogger()
		metricsHandler := metrics.NoopMetricsHandler
		out.conf = map[dynamicconfig.Key]any{}
		out.ms = workflow.NewMockMutableState(gomock.NewController(t))
		out.ms.EXPECT().VisitUpdates(gomock.Any())
		out.ms.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
		out.ms.EXPECT().GetCurrentVersion().Return(tests.LocalNamespaceEntry.FailoverVersion())

		out.updates = update.NewRegistry(out.ms)
		var effects effect.Buffer
		config := configs.NewConfig(
			dynamicconfig.NewCollection(
				dynamicconfig.StaticClient(out.conf), logger), 1)
		mockMeta := persistence.NewMockMetadataManager(gomock.NewController(t))
		nsReg := namespace.NewRegistry(
			mockMeta,
			true,
			func() time.Duration { return 1 * time.Hour },
			dynamicconfig.GetBoolPropertyFn(false),
			metricsHandler,
			logger,
		)
		out.handler = newWorkflowTaskCompletedHandler( // 😲
			t.Name(), // identity
			123,      // workflowTaskCompletedID
			out.ms,
			out.updates,
			&effects,
			newCommandAttrValidator(
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
		err = upd.Admit(context.Background(), req, workflow.WithEffects(effect.Immediate(context.Background()), tc.handler.mutableState))
		require.NoError(t, err)
		_ = upd.Send(context.Background(), true, &protocolpb.Message_EventId{EventId: 2208})

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

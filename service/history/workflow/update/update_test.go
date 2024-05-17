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

package update_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"go.temporal.io/server/service/history/consts"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

const (
	testAcceptedEventID   int64 = 1234
	testSequencingEventID int64 = 2203
)

func successOutcome(t *testing.T, s string) *updatepb.Outcome {
	return &updatepb.Outcome{
		Value: &updatepb.Outcome_Success{
			Success: payloads.EncodeString(t.Name() + s),
		},
	}
}

var eventStoreUnused update.EventStore

type mockEventStore struct {
	effect.Controller
	AddWorkflowExecutionUpdateAcceptedEventFunc func(
		updateID string,
		acceptedRequestMessageId string,
		acceptedRequestSequencingEventId int64,
		acceptedRequest *updatepb.Request,
	) (*historypb.HistoryEvent, error)

	AddWorkflowExecutionUpdateCompletedEventFunc func(
		acceptedEventID int64,
		resp *updatepb.Response,
	) (*historypb.HistoryEvent, error)

	CanAddEventFunc func() bool
}

func (m mockEventStore) AddWorkflowExecutionUpdateAcceptedEvent(
	updateID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateAcceptedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateAcceptedEventFunc(updateID, acceptedRequestMessageId, acceptedRequestSequencingEventId, acceptedRequest)
	}
	return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
}

func (m mockEventStore) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	resp *updatepb.Response,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateCompletedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateCompletedEventFunc(acceptedEventID, resp)
	}
	return &historypb.HistoryEvent{}, nil
}

func (m mockEventStore) CanAddEvent() bool {
	if m.CanAddEventFunc != nil {
		return m.CanAddEventFunc()
	}
	return true
}

func TestNilMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	upd := update.New(t.Name() + "update-id")
	err := upd.OnProtocolMessage(ctx, nil, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestNilMessageBody(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	upd := update.New(t.Name() + "update-id")
	emptyMsg := &protocolpb.Message{}
	err := upd.OnProtocolMessage(ctx, emptyMsg, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestUnsupportedMessageType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	upd := update.New(t.Name() + "update-id")
	msg := protocolpb.Message{}
	msg.Body = mustMarshalAny(t, &historypb.HistoryEvent{})
	err := upd.OnProtocolMessage(ctx, &msg, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestAdmitSendAcceptComplete(t *testing.T) {
	// this is the most common happy path - an update is created, requested,
	// accepted, and finally completed
	t.Parallel()
	var (
		ctx        = context.Background()
		completed  = false
		effects    = effect.Buffer{}
		invalidArg *serviceerror.InvalidArgument
		meta       = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req        = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt       = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         "random",
			AcceptedRequestSequencingEventId: 2208,
		})}
		resp         = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")})}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}

		completedEventData *updatepb.Response
		acceptedEventData  = struct {
			updateID                         string
			acceptedRequestMessageId         string
			acceptedRequestSequencingEventId int64
			acceptedRequest                  *updatepb.Request
		}{}
		store = mockEventStore{
			Controller: &effects,
			AddWorkflowExecutionUpdateAcceptedEventFunc: func(
				updateID string,
				acceptedRequestMessageId string,
				acceptedRequestSequencingEventId int64,
				acceptedRequest *updatepb.Request,
			) (*historypb.HistoryEvent, error) {
				acceptedEventData.updateID = updateID
				acceptedEventData.acceptedRequestMessageId = acceptedRequestMessageId
				acceptedEventData.acceptedRequestSequencingEventId = acceptedRequestSequencingEventId
				acceptedEventData.acceptedRequest = acceptedRequest
				return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
			},
			AddWorkflowExecutionUpdateCompletedEventFunc: func(
				acceptedEventID int64,
				res *updatepb.Response,
			) (*historypb.HistoryEvent, error) {
				completedEventData = res
				return &historypb.HistoryEvent{}, nil
			},
		}
		upd = update.New(meta.UpdateId, update.ObserveCompletion(&completed))
	)

	t.Run("admit", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &acpt, store)
		require.ErrorAs(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Created state", &acpt)

		err = upd.OnProtocolMessage(ctx, &resp, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Created state", &resp)

		err = upd.Admit(ctx, &req, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnProtocolMessage(ctx, &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in ProvisionallyAdmitted state", &resp)

		effects.Apply(ctx)
		t.Log("update state should now be Admitted")
	})

	msg := upd.Send(ctx, false, sequencingID)
	require.NotNil(t, msg)
	effects.Apply(ctx)

	t.Run("accept", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &acpt, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnProtocolMessage(ctx, &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T in while ProvisionallyAccepted state", &resp)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update acceptance should not be observable until effects are applied")
		require.Nil(t, status)

		effects.Apply(ctx)
		t.Log("update state should now be Accepted")

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome,
			"update is accepted but not completed so outcome should be nil")
	})

	t.Run("respond", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &resp, store)
		require.NoError(t, err)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update outcome should not be observable until effects are applied")

		effects.Apply(ctx)
		t.Log("update state should now be Completed")

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, mustUnmarshalBody[*updatepb.Response](t, &resp).Outcome, status.Outcome)
		require.True(t, completed)
	})

	require.Equal(t, meta.UpdateId, acceptedEventData.updateID)
	require.Equal(t, mustUnmarshalBody[*updatepb.Acceptance](t, &acpt).AcceptedRequestSequencingEventId, acceptedEventData.acceptedRequestSequencingEventId)
	protorequire.ProtoEqual(t, mustUnmarshalBody[*updatepb.Response](t, &resp), completedEventData)
}

func TestAdmitSendReject(t *testing.T) {
	t.Parallel()
	var (
		ctx       = context.Background()
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		updateID  = t.Name() + "-update-id"
		upd       = update.New(updateID, update.ObserveCompletion(&completed))
		req       = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	t.Run("admit", func(t *testing.T) {
		err := upd.Admit(ctx, &req, store)
		require.NoError(t, err)
		require.False(t, completed)
		effects.Apply(ctx)
		t.Log("update state should now be Admitted")
	})

	msg := upd.Send(ctx, false, sequencingID)
	require.NotNil(t, msg)
	effects.Apply(ctx)

	t.Run("reject", func(t *testing.T) {
		rej := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		})}
		err := upd.OnProtocolMessage(ctx, &rej, store)
		require.NoError(t, err)
		require.False(t, completed)

		{
			ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
			t.Cleanup(cncl)
			_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}
		{
			ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
			t.Cleanup(cncl)
			_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}

		effects.Apply(ctx)
		t.Log("update state should now be Completed")

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, mustUnmarshalBody[*updatepb.Rejection](t, &rej).Failure, status.Outcome.GetFailure())

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, mustUnmarshalBody[*updatepb.Rejection](t, &rej).Failure, status.Outcome.GetFailure())

		require.True(t, completed)
	})
}

func TestAdmitAcceptOrReject(t *testing.T) {
	// See comment in onAcceptanceMsg about stateAdmitted.
	// This test is to ensure that transition from stateAdmitted to stateAccepted or stateCompleted (rejected) is valid.

	t.Parallel()
	var (
		ctx     = context.Background()
		effects = effect.Immediate(ctx)
		meta    = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req     = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		store   = mockEventStore{
			Controller: effects,
			AddWorkflowExecutionUpdateAcceptedEventFunc: func(
				updateID string,
				acceptedRequestMessageId string,
				acceptedRequestSequencingEventId int64,
				acceptedRequest *updatepb.Request,
			) (*historypb.HistoryEvent, error) {
				return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
			},
		}
	)

	t.Run("accept admitted update", func(t *testing.T) {
		upd := update.New(meta.UpdateId)
		err := upd.Admit(ctx, &req, store)
		require.NoError(t, err)

		// Note: no upd.Send call.

		acpt := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         "random",
			AcceptedRequestSequencingEventId: 2208,
		})}
		err = upd.OnProtocolMessage(ctx, &acpt, store)
		require.NoError(t, err)

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome,
			"update is accepted but not completed so outcome should be nil")
	})

	t.Run("reject admitted update", func(t *testing.T) {
		upd := update.New(meta.UpdateId)
		err := upd.Admit(ctx, &req, store)
		require.NoError(t, err)

		// Note: no upd.Send call.

		rej := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		})}
		err = upd.OnProtocolMessage(ctx, &rej, store)
		require.NoError(t, err)

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, mustUnmarshalBody[*updatepb.Rejection](t, &rej).Failure, status.Outcome.GetFailure())
	})

}

func TestWithProtocolMessage(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		store    = mockEventStore{Controller: &effect.Buffer{}}
		updateID = t.Name() + "-update-id"
		upd      = update.NewAccepted(updateID, 2208)
		resp     = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		})}
	)

	t.Run("good message", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &resp, store)
		require.NoError(t, err)
	})
	t.Run("junk message", func(t *testing.T) {
		protocolMsg := &protocolpb.Message{
			Body: &anypb.Any{
				TypeUrl: "nonsense",
				Value:   []byte("even more nonsense"),
			},
		}
		err := upd.OnProtocolMessage(ctx, protocolMsg, store)
		require.Error(t, err)
	})
}

func TestSend(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		effects  effect.Buffer
		store    = mockEventStore{Controller: &effects}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	t.Run("before request received", func(t *testing.T) {
		require.False(t, upd.NeedToSend(false))
		msg := upd.Send(ctx, false, sequencingID)
		require.Nil(t, msg)
		require.False(t, upd.IsSent())
	})
	t.Run("admitted", func(t *testing.T) {
		require.NoError(t, upd.Admit(ctx, &req, store))
		effects.Apply(ctx)
		require.True(t, upd.NeedToSend(false))
		msg := upd.Send(ctx, false, sequencingID)
		effects.Apply(ctx)
		require.NotNil(t, msg)
		require.Equal(t, msg.GetEventId(), testSequencingEventID)
		require.True(t, upd.IsSent())
	})
	t.Run("sent", func(t *testing.T) {
		require.False(t, upd.NeedToSend(false))
		msg := upd.Send(ctx, false, sequencingID)
		effects.Apply(ctx)
		require.Nil(t, msg)
		require.True(t, upd.IsSent())
		require.True(t, upd.NeedToSend(true))
		msg = upd.Send(ctx, true, sequencingID)
		effects.Apply(ctx)
		require.NotNil(t, msg)
		require.Equal(t, msg.GetEventId(), testSequencingEventID)
		require.True(t, upd.IsSent())
	})
	t.Run("after accepted", func(t *testing.T) {
		updAccepted1 := update.NewAccepted(updateID, testAcceptedEventID)
		require.False(t, upd.NeedToSend(false))
		msg := updAccepted1.Send(ctx, false, sequencingID)
		require.Nil(t, msg)
		require.False(t, updAccepted1.IsSent())

		updAccepted2 := update.NewAccepted(updateID, testAcceptedEventID)
		msg = updAccepted2.Send(ctx, true, sequencingID)
		require.Nil(t, msg)
		require.False(t, updAccepted2.IsSent())
	})
}

func TestRejectAfterAcceptFails(t *testing.T) {
	t.Parallel()
	var (
		ctx     = context.Background()
		effects = effect.Buffer{}
		store   = mockEventStore{Controller: &effects}
	)

	updateID := t.Name() + "-update-id"
	upd := update.NewAccepted(updateID, testAcceptedEventID)
	err := upd.OnProtocolMessage(ctx, &protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{})}, store)
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
	require.ErrorContains(t, err, "state")
}

func TestAcceptanceAndResponseInSameMessageBatch(t *testing.T) {
	// it is possible for the acceptance message and the completion message to
	// be part of the same message batch, and thus they will be delivered without
	// an intermediate call to apply pending effects
	t.Parallel()
	var (
		ctx       = context.Background()
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         "x",
			AcceptedRequestSequencingEventId: testSequencingEventID,
			AcceptedRequest:                  &req,
		})}
		resp         = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")})}
		upd          = update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	require.NoError(t, upd.Admit(ctx, &req, store))
	effects.Apply(ctx)

	_ = upd.Send(ctx, false, sequencingID)
	effects.Apply(ctx)

	require.NoError(t, upd.OnProtocolMessage(ctx, &acpt, store))
	// no call to effects.Apply between these messages
	require.NoError(t, upd.OnProtocolMessage(ctx, &resp, store))
	require.False(t, completed)
	effects.Apply(ctx)
	require.True(t, completed)
}

func TestDuplicateRequestNoError(t *testing.T) {
	t.Parallel()
	var (
		ctx          = context.Background()
		updateID     = t.Name() + "-update-id"
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
		upd          = update.NewAccepted(updateID, testAcceptedEventID)
	)

	err := upd.Admit(ctx, &updatepb.Request{}, eventStoreUnused)
	require.NoError(t, err,
		"a second request should be ignored, not cause an error")

	msg := upd.Send(ctx, false, sequencingID)
	require.Nil(t, msg)
}

func TestMessageValidation(t *testing.T) {
	t.Parallel()
	var (
		ctx          = context.Background()
		updateID     = t.Name() + "-update-id"
		meta         = updatepb.Meta{UpdateId: updateID}
		req          = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
		store        = mockEventStore{Controller: effect.Immediate(ctx)}
		invalidArg   *serviceerror.InvalidArgument
	)

	t.Run("invalid request", func(t *testing.T) {
		upd := update.New("")
		err := upd.Admit(ctx, &updatepb.Request{}, eventStoreUnused)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
	t.Run("invalid accept msg", func(t *testing.T) {
		upd := update.New(updateID)
		err := upd.Admit(ctx, &req, store)
		require.NoError(t, err)
		_ = upd.Send(ctx, true, sequencingID)

		err = upd.OnProtocolMessage(
			ctx,
			&protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{})},
			store,
		)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid *update.Acceptance: accepted_request_sequencing_event_id is not set")

		err = upd.OnProtocolMessage(
			ctx,
			&protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
				AcceptedRequestSequencingEventId: testSequencingEventID,
			})},
			store,
		)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid *update.Acceptance: accepted_request_message_id is not set")
	})
	t.Run("invalid response msg", func(t *testing.T) {
		upd := update.NewAccepted("", testAcceptedEventID)
		err := upd.OnProtocolMessage(
			ctx,
			&protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{})},
			store,
		)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
}

func TestDoubleRollback(t *testing.T) {
	// Test that an Update that receives both an Acceptance and a Response in
	// the same message batch but is then rolled back ends up in the proper
	// state.
	t.Parallel()
	var (
		ctx       = context.Background()
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		reqMsgID  = t.Name() + "-req-msg-id"
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         reqMsgID,
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}
		resp         = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")})}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
	require.NoError(t, upd.Admit(ctx, &req, store))
	effects.Apply(ctx)

	_ = upd.Send(ctx, false, sequencingID)
	effects.Apply(ctx)

	require.NoError(t, upd.OnProtocolMessage(ctx, &acpt, store))
	require.NoError(t, upd.OnProtocolMessage(ctx, &resp, store))
	require.False(t, completed)

	t.Log("pretend MutableState write to DB fails - unwind effects")
	effects.Cancel(ctx)

	t.Run("not accepted", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("back to sent state", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &acpt, store)
		require.NoError(t, err, "update should be back in Sent state")
	})
}

func TestRollbackCompletion(t *testing.T) {
	t.Parallel()
	var (
		ctx       = context.Background()
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		updateID  = t.Name() + "-update-id"
		upd       = update.NewAccepted(
			updateID,
			testAcceptedEventID,
			update.ObserveCompletion(&completed),
		)
		resp = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		})}
	)

	require.NoError(t, upd.OnProtocolMessage(ctx, &resp, store))
	require.False(t, completed)

	t.Log("pretend MutableState write to DB fails - unwind effects")
	effects.Cancel(ctx)

	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, completed)
	})
	t.Run("back to accepted state", func(t *testing.T) {
		err := upd.OnProtocolMessage(ctx, &resp, store)
		require.NoError(t, err, "update should be back in Accepted state")
	})
}

func TestRejectionWithAcceptanceWaiter(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		store    = mockEventStore{Controller: effect.Immediate(ctx)}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not_empty"},
		}
		rej = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{
			RejectedRequestMessageId: "not_empty",
			RejectedRequest:          &req,
			Failure: &failurepb.Failure{
				Message: "intentional falure from " + t.Name(),
			},
		})}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)
	ch := make(chan any, 1)
	go func() {
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 1*time.Second)
		if err != nil {
			ch <- err
			return
		}
		ch <- status.Outcome
	}()
	t.Log("give 5ms for the goro to get to the Future.Get call in WaitLifecycleStage")
	time.Sleep(5 * time.Millisecond)

	t.Log("deliver request and rejection messages")
	require.NoError(t, upd.Admit(ctx, &req, store))

	_ = upd.Send(ctx, false, sequencingID)

	require.NoError(t, upd.OnProtocolMessage(ctx, &rej, store))

	retVal := <-ch
	outcome, ok := retVal.(*updatepb.Outcome)
	require.Truef(t, ok, "WaitLifecycleStage returned an unexpected type: %T", retVal)
	require.Equal(t, mustUnmarshalBody[*updatepb.Rejection](t, &rej).Failure, outcome.GetFailure())
}

func TestAcceptEventIDInCompletedEvent(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		effects  = effect.Buffer{}
		store    = mockEventStore{Controller: &effects}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not_empty"},
		}
		acpt = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         "not empty",
			AcceptedRequest:                  &req,
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}
		resp = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		})}
		wantAcceptedEventID int64 = 8675309
		sequencingID              = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	var gotAcceptedEventID int64
	store.AddWorkflowExecutionUpdateAcceptedEventFunc = func(
		updateID string,
		acceptedRequestMessageId string,
		acceptedRequestSequencingEventId int64,
		acceptedRequest *updatepb.Request,
	) (*historypb.HistoryEvent, error) {
		t.Log("creating accepted event with the desired event ID")
		return &historypb.HistoryEvent{EventId: wantAcceptedEventID}, nil
	}
	store.AddWorkflowExecutionUpdateCompletedEventFunc = func(
		acceptedEventID int64,
		resp *updatepb.Response,
	) (*historypb.HistoryEvent, error) {
		t.Log("capturing acceptedEventID written in completed event")
		gotAcceptedEventID = acceptedEventID
		return &historypb.HistoryEvent{}, nil
	}

	require.NoError(t, upd.Admit(ctx, &req, store))
	effects.Apply(ctx)
	_ = upd.Send(ctx, false, sequencingID)
	effects.Apply(ctx)
	require.NoError(t, upd.OnProtocolMessage(ctx, &acpt, store))
	require.NoError(t, upd.OnProtocolMessage(ctx, &resp, store))
	effects.Apply(ctx)
	require.Equal(t, wantAcceptedEventID, gotAcceptedEventID)
}

func TestWaitLifecycleStage(t *testing.T) {
	t.Parallel()
	const (
		serverTimeout = 10 * time.Millisecond
	)
	var (
		completed = false
		effects   = effect.Buffer{}
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         "random",
			AcceptedRequestSequencingEventId: 2208,
		})}
		rej = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		})}
		resp              = protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")})}
		acceptedEventData = struct {
			updateID                         string
			acceptedRequestMessageId         string
			acceptedRequestSequencingEventId int64
			acceptedRequest                  *updatepb.Request
		}{}
		store = mockEventStore{
			Controller: &effects,
			AddWorkflowExecutionUpdateAcceptedEventFunc: func(
				updateID string,
				acceptedRequestMessageId string,
				acceptedRequestSequencingEventId int64,
				acceptedRequest *updatepb.Request,
			) (*historypb.HistoryEvent, error) {
				acceptedEventData.updateID = updateID
				acceptedEventData.acceptedRequestMessageId = acceptedRequestMessageId
				acceptedEventData.acceptedRequestSequencingEventId = acceptedRequestSequencingEventId
				acceptedEventData.acceptedRequest = acceptedRequest
				return &historypb.HistoryEvent{EventId: testAcceptedEventID}, nil
			},
			AddWorkflowExecutionUpdateCompletedEventFunc: func(
				acceptedEventID int64,
				res *updatepb.Response,
			) (*historypb.HistoryEvent, error) {
				return &historypb.HistoryEvent{}, nil
			},
		}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	assertAdmitted := func(ctx context.Context, t *testing.T, upd *update.Update) {
		assert := func(status *update.Status, err error) {
			require.NoError(t, err)
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
			require.Nil(t, status.Outcome)
		}
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, time.Duration(0))
		assert(status, err)
	}

	assertAccepted := func(ctx context.Context, t *testing.T, upd *update.Update, alreadyInState bool) {
		assert := func(status *update.Status, err error) {
			require.NoError(t, err)
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
			require.Nil(t, status.Outcome)
		}
		if alreadyInState {
			status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, time.Duration(0))
			assert(status, err)
		}
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, serverTimeout)
		assert(status, err)
	}

	assertSuccess := func(ctx context.Context, t *testing.T, upd *update.Update, alreadyInState bool) {
		assert := func(status *update.Status, err error) {
			require.NoError(t, err)
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
			require.Nil(t, status.Outcome.GetFailure())
			require.NotNil(t, status.Outcome.GetSuccess())
			require.Equal(t, mustUnmarshalBody[*updatepb.Response](t, &resp).Outcome, status.Outcome)
		}
		if alreadyInState {
			status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, time.Duration(0))
			assert(status, err)
		}
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, serverTimeout)
		assert(status, err)
	}

	assertFailure := func(ctx context.Context, t *testing.T, upd *update.Update, alreadyInState bool) {
		assert := func(status *update.Status, err error) {
			require.NoError(t, err)
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
			require.NotNil(t, status.Outcome.GetFailure())
			require.Nil(t, status.Outcome.GetSuccess())
			require.Equal(t, mustUnmarshalBody[*updatepb.Rejection](t, &rej).Failure, status.Outcome.GetFailure())
		}
		if alreadyInState {
			status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, time.Duration(0))
			assert(status, err)
		}
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, serverTimeout)
		assert(status, err)
	}

	assertSoftTimeoutWhileWaitingForAccepted := func(ctx context.Context, t *testing.T, upd *update.Update) {
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, serverTimeout)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
		require.Nil(t, status.Outcome)
	}

	assertSoftTimeoutWhileWaitingForCompleted := func(ctx context.Context, t *testing.T, upd *update.Update) {
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, serverTimeout)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome)
	}

	assertContextDeadlineExceededWhileWaitingForAccepted := func(ctx context.Context, t *testing.T, upd *update.Update) {
		_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, serverTimeout)
		require.Error(t, err)
		require.Error(t, ctx.Err())
		require.True(t, common.IsContextDeadlineExceededErr(err))
		require.True(t, common.IsContextDeadlineExceededErr(ctx.Err()))
	}

	assertContextDeadlineExceededWhileWaitingForCompleted := func(ctx context.Context, t *testing.T, upd *update.Update) {
		_, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, serverTimeout)
		require.Error(t, err)
		require.Error(t, ctx.Err())
		require.True(t, common.IsContextDeadlineExceededErr(err))
		require.True(t, common.IsContextDeadlineExceededErr(ctx.Err()))
	}

	admit := func(ctx context.Context, req *updatepb.Request, upd *update.Update) {
		err := upd.Admit(ctx, req, store)
		require.NoError(t, err)
		effects.Apply(ctx)
	}

	applyMessage := func(ctx context.Context, msg *protocolpb.Message, upd *update.Update) {
		err := upd.OnProtocolMessage(ctx, msg, store)
		require.NoError(t, err)
		effects.Apply(ctx)
	}

	send := func(ctx context.Context, upd *update.Update) {
		_ = upd.Send(ctx, false, sequencingID)
		effects.Apply(ctx)
	}

	t.Run("test non-blocking calls (accepted)", func(t *testing.T) {
		ctx := context.Background()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		assertAdmitted(ctx, t, upd)
		admit(ctx, &req, upd)         // => Admitted
		send(ctx, upd)                // => Sent
		applyMessage(ctx, &acpt, upd) // => Accepted
		assertAccepted(ctx, t, upd, true)
		applyMessage(ctx, &resp, upd) // => Completed (success)
		assertSuccess(ctx, t, upd, true)
	})

	t.Run("test non-blocking calls (rejected)", func(t *testing.T) {
		ctx := context.Background()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		admit(ctx, &req, upd)        // => Admitted
		send(ctx, upd)               // => Sent
		applyMessage(ctx, &rej, upd) // => Completed (failure)
		assertFailure(ctx, t, upd, true)
	})

	t.Run("test blocking calls (accepted)", func(t *testing.T) {
		ctx := context.Background()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		admit(ctx, &req, upd) // => Admitted
		send(ctx, upd)        // => Sent
		done := make(chan any)
		go func() {
			assertAccepted(ctx, t, upd, false)
			close(done)
		}()
		applyMessage(ctx, &acpt, upd) // => Accepted
		<-done

		ctx = context.Background()
		done = make(chan any)
		go func() {
			assertSuccess(ctx, t, upd, false)
			close(done)
		}()
		applyMessage(ctx, &resp, upd) // => Completed (success)
		<-done
	})

	t.Run("test blocking calls (rejected)", func(t *testing.T) {
		ctx := context.Background()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		admit(ctx, &req, upd) // => Admitted
		send(ctx, upd)        // => Sent
		done := make(chan any)
		go func() {
			assertFailure(ctx, t, upd, false)
			close(done)
		}()
		applyMessage(ctx, &rej, upd) // => Completed (failure)
		<-done
	})

	t.Run("timeout is soft iff due to server-imposed deadline (accepted)", func(t *testing.T) {
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		// Use 10x user timeout as a buffer to make sure that it won't be exceeded before serverTimeout.
		ctx, cancel := context.WithTimeout(context.Background(), serverTimeout*10)
		defer cancel()
		assertSoftTimeoutWhileWaitingForAccepted(ctx, t, upd)
		ctx, cancel = context.WithTimeout(context.Background(), serverTimeout/2)
		defer cancel()
		assertContextDeadlineExceededWhileWaitingForAccepted(ctx, t, upd)
	})

	t.Run("timeout is soft iff due to server-imposed deadline (completed)", func(t *testing.T) {
		// Use 10x user timeout as a buffer to make sure that it won't be exceeded before serverTimeout.
		ctx, cancel := context.WithTimeout(context.Background(), serverTimeout*10)
		defer cancel()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		assertAdmitted(ctx, t, upd)
		admit(ctx, &req, upd)         // => Admitted
		send(ctx, upd)                // => Sent
		applyMessage(ctx, &acpt, upd) // => Accepted
		assertAccepted(ctx, t, upd, true)
		assertSoftTimeoutWhileWaitingForCompleted(ctx, t, upd)
		ctx, cancel = context.WithTimeout(context.Background(), serverTimeout/2)
		defer cancel()
		assertContextDeadlineExceededWhileWaitingForCompleted(ctx, t, upd)
	})

	t.Run("wait for Unspecified or Admitted states returns current status immediately", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0))
		defer cancel()
		upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
		require.Nil(t, status.Outcome)

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, status.Stage)
		require.Nil(t, status.Outcome)

		admit(ctx, &req, upd)         // => Admitted
		send(ctx, upd)                // => Sent
		applyMessage(ctx, &acpt, upd) // => Accepted

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome)

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, status.Stage)
		require.Nil(t, status.Outcome)

		applyMessage(ctx, &resp, upd) // => Completed (success)

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.NotNil(t, status.Outcome)

		status, err = upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.NotNil(t, status.Outcome)
	})
}

func TestCompletedWorkflow(t *testing.T) {
	var (
		ctx     = context.Background()
		effects = effect.Immediate(ctx)

		meta = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req  = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}

		store   = mockEventStore{Controller: effects}
		roStore = mockEventStore{Controller: effects}
	)
	roStore.CanAddEventFunc = func() bool { return false }

	t.Run("admit returns error if workflow is completed", func(t *testing.T) {
		upd := update.New(meta.UpdateId)
		err := upd.Admit(ctx, &req, roStore)
		require.Error(t, err)
		require.ErrorIs(t, err, consts.ErrWorkflowCompleted)

		oneMsCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
		defer cancel()
		status, err := upd.WaitLifecycleStage(oneMsCtx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.Error(t, err)
		require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
		require.Nil(t, status)
	})

	t.Run("sent update is aborted if completed workflow tries to accept it", func(t *testing.T) {
		upd := update.New(meta.UpdateId)
		_ = upd.Admit(ctx, &req, store)
		upd.Send(ctx, false, &protocolpb.Message_EventId{EventId: testSequencingEventID})

		acpt := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Acceptance{AcceptedRequestSequencingEventId: 2208})}
		err := upd.OnProtocolMessage(ctx, &acpt, roStore)
		require.NoError(t, err)

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.Error(t, err)
		require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
		require.Nil(t, status)
	})

	t.Run("sent update is rejected with user rejection if completed workflow rejects it", func(t *testing.T) {
		upd := update.New(meta.UpdateId)
		_ = upd.Admit(ctx, &req, store)
		upd.Send(ctx, false, &protocolpb.Message_EventId{EventId: testSequencingEventID})

		rej := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		})}
		err := upd.OnProtocolMessage(ctx, &rej, roStore)
		require.NoError(t, err)

		oneMsCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
		defer cancel()
		status, err := upd.WaitLifecycleStage(oneMsCtx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
		require.Equal(t, "An intentional failure", status.Outcome.GetFailure().GetMessage())
	})

	t.Run("accepted update is rejected if completed workflow completes it", func(t *testing.T) {
		upd := update.NewAccepted(meta.UpdateId, testAcceptedEventID)

		resp := protocolpb.Message{Body: mustMarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")})}
		err := upd.OnProtocolMessage(ctx, &resp, roStore)
		require.NoError(t, err)

		status, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.Error(t, err)
		require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
		require.Nil(t, status)
	})
}

func mustMarshalAny(t *testing.T, pb proto.Message) *anypb.Any {
	t.Helper()
	var a anypb.Any
	require.NoError(t, a.MarshalFrom(pb))
	return &a
}

func mustUnmarshalBody[T proto.Message](t *testing.T, protocolMsg *protocolpb.Message) T {
	t.Helper()
	pb := new(T)
	ppb := reflect.ValueOf(pb).Elem()
	pbNew := reflect.New(reflect.TypeOf(pb).Elem().Elem())
	ppb.Set(pbNew)

	require.NoError(t, protocolMsg.Body.UnmarshalTo(*pb))
	return *pb
}

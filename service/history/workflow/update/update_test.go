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
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"

	"go.temporal.io/server/common/payloads"
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

func TestNilMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	upd := update.New(t.Name() + "update-id")
	err := upd.OnMessage(ctx, nil, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestUnsupportedMessageType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	upd := update.New(t.Name() + "update-id")
	notAMessageType := historypb.HistoryEvent{}
	err := upd.OnMessage(ctx, &notAMessageType, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestRequestAcceptComplete(t *testing.T) {
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
		acpt       = updatepb.Acceptance{AcceptedRequestSequencingEventId: 2208}
		resp       = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}

		completedEventData updatepb.Response
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
				completedEventData = *res
				return &historypb.HistoryEvent{}, nil
			},
		}
		upd = update.New(meta.UpdateId, update.ObserveCompletion(&completed))
	)

	t.Run("request", func(t *testing.T) {
		err := upd.OnMessage(ctx, &acpt, store)
		require.ErrorAs(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Admitted state", &acpt)

		err = upd.OnMessage(ctx, &resp, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Admitted state", &resp)

		err = upd.OnMessage(ctx, &req, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnMessage(ctx, &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in ProvisionallyRequested state", &resp)

		effects.Apply(ctx)
		t.Log("update state should now be Requested")
	})

	t.Run("accept", func(t *testing.T) {
		err := upd.OnMessage(ctx, &acpt, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnMessage(ctx, &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T in while ProvisionallyAccepted state", &resp)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err = upd.WaitAccepted(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update acceptance should not be observable until effects are applied")

		effects.Apply(ctx)
		t.Log("update state should now be Accepted")

		outcome, err := upd.WaitAccepted(ctx)
		require.NoError(t, err)
		require.Nil(t, outcome,
			"update is accepted but not completed so outcome should be nil")
	})

	t.Run("respond", func(t *testing.T) {
		err := upd.OnMessage(ctx, &resp, store)
		require.NoError(t, err)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err = upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update outcome should not be observable until effects are applied")

		effects.Apply(ctx)
		t.Log("update state should now be Completed")

		gotOutcome, err := upd.WaitOutcome(ctx)
		require.NoError(t, err)
		require.Equal(t, resp.Outcome, gotOutcome)
		require.True(t, completed)
	})

	require.Equal(t, meta.UpdateId, acceptedEventData.updateID)
	require.Equal(t, acpt.AcceptedRequestSequencingEventId, acceptedEventData.acceptedRequestSequencingEventId)
	require.Equal(t, resp, completedEventData)
}

func TestRequestReject(t *testing.T) {
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
	)

	t.Run("request", func(t *testing.T) {
		err := upd.OnMessage(ctx, &req, store)
		require.NoError(t, err)
		require.False(t, completed)
		effects.Apply(ctx)
		t.Log("update state should now be Requested")
	})

	t.Run("reject", func(t *testing.T) {
		rej := updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		}
		err := upd.OnMessage(ctx, &rej, store)
		require.NoError(t, err)
		require.False(t, completed)

		{
			ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
			t.Cleanup(cncl)
			_, err := upd.WaitAccepted(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}
		{
			ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
			t.Cleanup(cncl)
			_, err := upd.WaitOutcome(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}

		effects.Apply(ctx)
		t.Log("update state should now be Completed")

		acptOutcome, err := upd.WaitAccepted(ctx)
		require.NoError(t, err)
		require.Equal(t, rej.Failure, acptOutcome.GetFailure())

		outcome, err := upd.WaitOutcome(ctx)
		require.NoError(t, err)
		require.Equal(t, rej.Failure, outcome.GetFailure())

		require.True(t, completed)
	})
}

func TestWithProtocolMessage(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		store    = mockEventStore{Controller: &effect.Buffer{}}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
	)

	t.Run("good message", func(t *testing.T) {
		protocolMsg := &protocolpb.Message{Body: mustMarshalAny(t, &req)}
		err := upd.OnMessage(ctx, protocolMsg, store)
		require.NoError(t, err)
	})
	t.Run("junk message", func(t *testing.T) {
		protocolMsg := &protocolpb.Message{
			Body: &types.Any{
				TypeUrl: "nonsense",
				Value:   []byte("even more nonsense"),
			},
		}
		err := upd.OnMessage(ctx, protocolMsg, store)
		require.Error(t, err)
	})
}

func TestMessageOutput(t *testing.T) {
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
		msgs := make([]*protocolpb.Message, 0)
		upd.ReadOutgoingMessages(&msgs, sequencingID)
		require.Empty(t, msgs)
	})
	t.Run("requested", func(t *testing.T) {
		require.NoError(t, upd.OnMessage(ctx, &req, store))
		effects.Apply(ctx)
		msgs := make([]*protocolpb.Message, 0)
		upd.ReadOutgoingMessages(&msgs, sequencingID)
		require.Len(t, msgs, 1)
		require.Equal(t, msgs[0].GetEventId(), testSequencingEventID)
	})
	t.Run("after requested", func(t *testing.T) {
		upd := update.NewAccepted(updateID, testAcceptedEventID)
		msgs := make([]*protocolpb.Message, 0)
		upd.ReadOutgoingMessages(&msgs, sequencingID)
		require.Empty(t, msgs)
	})
}

func TestRejectAfterAcceptFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	updateID := t.Name() + "-update-id"
	upd := update.NewAccepted(updateID, testAcceptedEventID)
	err := upd.OnMessage(ctx, &updatepb.Rejection{}, eventStoreUnused)
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
	require.ErrorContains(t, err, "state")
}

func TestAcceptanceAndResponseInSameMessageBatch(t *testing.T) {
	// it is possible for the acceptance message and the completion message to
	// be part of the same message batch and thus they will be delivered without
	// an intermediate call to apply pending effects
	t.Parallel()
	var (
		ctx       = context.Background()
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = updatepb.Acceptance{AcceptedRequest: &req, AcceptedRequestMessageId: "x"}
		resp      = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}
		upd       = update.New(meta.UpdateId, update.ObserveCompletion(&completed))
	)

	require.NoError(t, upd.OnMessage(ctx, &req, store))
	effects.Apply(ctx)

	require.NoError(t, upd.OnMessage(ctx, &acpt, store))
	// no call to effects.Apply between these messages
	require.NoError(t, upd.OnMessage(ctx, &resp, store))
	require.False(t, completed)
	effects.Apply(ctx)
	require.True(t, completed)
}

func TestDuplicateRequestNoError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	updateID := t.Name() + "-update-id"
	sequencingID := &protocolpb.Message_EventId{EventId: testSequencingEventID}
	upd := update.NewAccepted(updateID, testAcceptedEventID)
	err := upd.OnMessage(ctx, &updatepb.Request{}, eventStoreUnused)
	require.NoError(t, err,
		"a second request message should be ignored, not cause an error")

	msgs := make([]*protocolpb.Message, 0)
	upd.ReadOutgoingMessages(&msgs, sequencingID)
	require.Empty(t, msgs)
}

func TestMessageValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var invalidArg *serviceerror.InvalidArgument
	updateID := t.Name() + "-update-id"
	t.Run("invalid request msg", func(t *testing.T) {
		upd := update.New("")
		err := upd.OnMessage(ctx, &updatepb.Request{}, eventStoreUnused)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
	t.Run("invalid acceptance msg", func(t *testing.T) {
		upd := update.New(updateID)
		store := mockEventStore{Controller: effect.Immediate(ctx)}
		validReq := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not empty"},
		}
		err := upd.OnMessage(ctx, &validReq, store)
		require.NoError(t, err)
		err = upd.OnMessage(ctx, nil, store)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "received nil message")
	})
	t.Run("invalid rejection msg", func(t *testing.T) {
		upd := update.New(updateID)
		store := mockEventStore{
			Controller: effect.Immediate(ctx),
		}
		validReq := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not empty"},
		}
		err := upd.OnMessage(ctx, &validReq, store)
		require.NoError(t, err)
		err = upd.OnMessage(ctx, nil, store)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "received nil message")
	})
	t.Run("invalid response msg", func(t *testing.T) {
		upd := update.NewAccepted("", testAcceptedEventID)
		err := upd.OnMessage(
			ctx,
			&updatepb.Response{},
			eventStoreUnused,
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
		acpt      = updatepb.Acceptance{AcceptedRequest: &req, AcceptedRequestMessageId: reqMsgID}
		resp      = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}
	)

	upd := update.New(meta.UpdateId, update.ObserveCompletion(&completed))
	require.NoError(t, upd.OnMessage(ctx, &req, store))
	effects.Apply(ctx)

	require.NoError(t, upd.OnMessage(ctx, &acpt, store))
	require.NoError(t, upd.OnMessage(ctx, &resp, store))
	require.False(t, completed)

	t.Log("pretend MutableState write to DB fails - unwind effects")
	effects.Cancel(ctx)

	t.Run("not accepted", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitAccepted(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("back to requested state", func(t *testing.T) {
		err := upd.OnMessage(ctx, &acpt, store)
		require.NoError(t, err, "update should be back in Requested state")
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
		resp = updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		}
	)

	require.NoError(t, upd.OnMessage(ctx, &resp, store))
	require.False(t, completed)

	t.Log("pretend MutableState write to DB fails - unwind effects")
	effects.Cancel(ctx)

	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(ctx, 5*time.Millisecond)
		t.Cleanup(cncl)
		_, err := upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, completed)
	})
	t.Run("back to accepted state", func(t *testing.T) {
		err := upd.OnMessage(ctx, &resp, store)
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
		rej = updatepb.Rejection{
			RejectedRequestMessageId: "not_empty",
			RejectedRequest:          &req,
			Failure: &failurepb.Failure{
				Message: "intentional falure from " + t.Name(),
			},
		}
	)
	ch := make(chan any, 1)
	go func() {
		fail, err := upd.WaitAccepted(ctx)
		if err != nil {
			ch <- err
			return
		}
		ch <- fail
	}()
	t.Log("give 5ms for the goro to get to the Future.Get call in WaitAccepted")
	time.Sleep(5 * time.Millisecond)

	t.Log("deliver request and rejection messages")
	require.NoError(t, upd.OnMessage(ctx, &req, store))
	require.NoError(t, upd.OnMessage(ctx, &rej, store))

	retVal := <-ch
	outcome, ok := retVal.(*updatepb.Outcome)
	require.Truef(t, ok, "WaitAccepted returned an unexpected type: %T", retVal)
	require.Equal(t, rej.Failure, outcome.GetFailure())
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
		acpt = updatepb.Acceptance{
			AcceptedRequestMessageId: "not empty",
			AcceptedRequest:          &req,
		}
		resp = updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		}
		wantAcceptedEventID int64 = 8675309
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

	require.NoError(t, upd.OnMessage(ctx, &req, store))
	effects.Apply(ctx)
	require.NoError(t, upd.OnMessage(ctx, &acpt, store))
	require.NoError(t, upd.OnMessage(ctx, &resp, store))
	effects.Apply(ctx)
	require.Equal(t, wantAcceptedEventID, gotAcceptedEventID)
}

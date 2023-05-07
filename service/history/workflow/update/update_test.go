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

func successOutcome(t *testing.T, s string) *updatepb.Outcome {
	return &updatepb.Outcome{
		Value: &updatepb.Outcome_Success{
			Success: payloads.EncodeString(t.Name() + s),
		},
	}
}

func ignoreCompletion() {}

var eventStoreUnused update.EventStore

type mockEventStore struct {
	effect.Controller
	AddWorkflowExecutionUpdateAcceptedEventFunc func(
		updateID string,
		acpt *updatepb.Acceptance,
	) (*historypb.HistoryEvent, error)

	AddWorkflowExecutionUpdateCompletedEventFunc func(
		resp *updatepb.Response,
	) (*historypb.HistoryEvent, error)
}

func (m mockEventStore) AddWorkflowExecutionUpdateAcceptedEvent(
	updateID string,
	acpt *updatepb.Acceptance,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateAcceptedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateAcceptedEventFunc(updateID, acpt)
	}
	return nil, nil
}

func (m mockEventStore) AddWorkflowExecutionUpdateCompletedEvent(
	resp *updatepb.Response,
) (*historypb.HistoryEvent, error) {
	if m.AddWorkflowExecutionUpdateCompletedEventFunc != nil {
		return m.AddWorkflowExecutionUpdateCompletedEventFunc(resp)
	}
	return nil, nil
}

func TestNilMessage(t *testing.T) {
	t.Parallel()
	upd := update.New(t.Name()+"update-id", ignoreCompletion)
	err := upd.OnMessage(context.TODO(), nil, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestUnsupportedMessageType(t *testing.T) {
	t.Parallel()
	upd := update.New(t.Name()+"update-id", ignoreCompletion)
	notAMessageType := historypb.HistoryEvent{}
	err := upd.OnMessage(context.TODO(), &notAMessageType, mockEventStore{})
	var invalidArg *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArg)
}

func TestRequestAcceptComplete(t *testing.T) {
	// this is the most common happy path - an update is created, requested,
	// accepted, and finally completed
	t.Parallel()
	var (
		completed  = false
		effects    = effect.Buffer{}
		invalidArg *serviceerror.InvalidArgument
		meta       = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req        = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt       = updatepb.Acceptance{AcceptedRequest: &req, AcceptedRequestMessageId: "x"}
		resp       = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}

		completedEventData updatepb.Response
		acceptedEventData  = struct {
			updateID string
			acpt     updatepb.Acceptance
		}{}
		store = mockEventStore{
			Controller: &effects,
			AddWorkflowExecutionUpdateAcceptedEventFunc: func(
				updateID string,
				acpt *updatepb.Acceptance,
			) (*historypb.HistoryEvent, error) {
				acceptedEventData.updateID = updateID
				acceptedEventData.acpt = *acpt
				return nil, nil
			},
			AddWorkflowExecutionUpdateCompletedEventFunc: func(
				res *updatepb.Response,
			) (*historypb.HistoryEvent, error) {
				completedEventData = *res
				return nil, nil
			},
		}
		upd = update.New(meta.UpdateId, func() { completed = true })
	)

	t.Run("request", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &acpt, store)
		require.ErrorAs(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Admitted state", &acpt)

		err = upd.OnMessage(context.TODO(), &resp, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in Admitted state", &resp)

		err = upd.OnMessage(context.TODO(), &req, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnMessage(context.TODO(), &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T while in ProvisionallyRequested state", &resp)

		effects.Apply(context.TODO())
		t.Log("update state should now be Requested")
	})

	t.Run("accept", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &acpt, store)
		require.NoError(t, err)
		require.False(t, completed)

		err = upd.OnMessage(context.TODO(), &acpt, store)
		require.ErrorAsf(t, err, &invalidArg,
			"expected InvalidArgument from %T in while ProvisionallyAccepted state", &resp)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err = upd.WaitAccepted(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update acceptance should not be observable until effects are applied")

		effects.Apply(context.TODO())
		t.Log("update state should now be Accepted")

		outcome, err := upd.WaitAccepted(context.TODO())
		require.NoError(t, err)
		require.Nil(t, outcome,
			"update is accepted but not completed so outcome should be nil")
	})

	t.Run("respond", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &resp, store)
		require.NoError(t, err)
		require.False(t, completed)

		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err = upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"update outcome should not be observable until effects are applied")

		effects.Apply(context.TODO())
		t.Log("update state should now be Completed")

		gotOutcome, err := upd.WaitOutcome(context.TODO())
		require.NoError(t, err)
		require.Equal(t, resp.Outcome, gotOutcome)
		require.True(t, completed)
	})

	require.Equal(t, meta.UpdateId, acceptedEventData.updateID)
	require.Equal(t, acpt, acceptedEventData.acpt)
	require.Equal(t, resp, completedEventData)
}

func TestRequestReject(t *testing.T) {
	t.Parallel()
	var (
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		updateID  = t.Name() + "-update-id"
		upd       = update.New(updateID, func() { completed = true })
		req       = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
	)

	t.Run("request", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &req, store)
		require.NoError(t, err)
		require.False(t, completed)
		effects.Apply(context.TODO())
		t.Log("update state should now be Requested")
	})

	t.Run("reject", func(t *testing.T) {
		rej := updatepb.Rejection{
			RejectedRequest: &req,
			Failure:         &failurepb.Failure{Message: "An intentional failure"},
		}
		err := upd.OnMessage(context.TODO(), &rej, store)
		require.NoError(t, err)
		require.False(t, completed)

		{
			ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cncl()
			_, err := upd.WaitAccepted(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}
		{
			ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cncl()
			_, err := upd.WaitOutcome(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"update acceptance failure should not be observable until effects are applied")
		}

		effects.Apply(context.TODO())
		t.Log("update state should now be Completed")

		acptOutcome, err := upd.WaitAccepted(context.TODO())
		require.NoError(t, err)
		require.Equal(t, rej.Failure, acptOutcome.GetFailure())

		outcome, err := upd.WaitOutcome(context.TODO())
		require.NoError(t, err)
		require.Equal(t, rej.Failure, outcome.GetFailure())

		require.True(t, completed)
	})
}

func TestWithProtocolMessage(t *testing.T) {
	t.Parallel()
	var (
		store    = mockEventStore{Controller: &effect.Buffer{}}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID, ignoreCompletion)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
	)

	t.Run("good message", func(t *testing.T) {
		protocolMsg := &protocolpb.Message{Body: mustMarshalAny(t, &req)}
		err := upd.OnMessage(context.TODO(), protocolMsg, store)
		require.NoError(t, err)
	})
	t.Run("junk message", func(t *testing.T) {
		protocolMsg := &protocolpb.Message{
			Body: &types.Any{
				TypeUrl: "nonsense",
				Value:   []byte("even more nonsense"),
			},
		}
		err := upd.OnMessage(context.TODO(), protocolMsg, store)
		require.Error(t, err)
	})
}

func TestMessageOutput(t *testing.T) {
	t.Parallel()
	var (
		effects  effect.Buffer
		store    = mockEventStore{Controller: &effects}
		updateID = t.Name() + "-update-id"
		upd      = update.New(updateID, ignoreCompletion)
		req      = updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: t.Name()},
		}
	)

	t.Run("before request received", func(t *testing.T) {
		msgs := make([]*protocolpb.Message, 0)
		upd.PollOutboundMessages(&msgs)
		require.Empty(t, msgs)
	})
	t.Run("requested", func(t *testing.T) {
		require.NoError(t, upd.OnMessage(context.TODO(), &req, store))
		effects.Apply(context.TODO())
		msgs := make([]*protocolpb.Message, 0)
		upd.PollOutboundMessages(&msgs)
		require.Len(t, msgs, 1)
	})
	t.Run("after requested", func(t *testing.T) {
		upd := update.NewAccepted(updateID, ignoreCompletion)
		msgs := make([]*protocolpb.Message, 0)
		upd.PollOutboundMessages(&msgs)
		require.Empty(t, msgs)
	})
}

func TestRejectAfterAcceptFails(t *testing.T) {
	t.Parallel()
	updateID := t.Name() + "-update-id"
	upd := update.NewAccepted(updateID, ignoreCompletion)
	err := upd.OnMessage(context.TODO(), &updatepb.Rejection{}, eventStoreUnused)
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
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = updatepb.Acceptance{AcceptedRequest: &req, AcceptedRequestMessageId: "x"}
		resp      = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}
		upd       = update.New(meta.UpdateId, func() { completed = true })
	)

	require.NoError(t, upd.OnMessage(context.TODO(), &req, store))
	effects.Apply(context.TODO())

	require.NoError(t, upd.OnMessage(context.TODO(), &acpt, store))
	// no call to effects.Apply between these messages
	require.NoError(t, upd.OnMessage(context.TODO(), &resp, store))
	require.False(t, completed)
	effects.Apply(context.TODO())
	require.True(t, completed)
}

func TestCompletedLazyOutcomeRead(t *testing.T) {
	t.Parallel()
	updateID := t.Name() + "-update-id"
	outcome := successOutcome(t, "success!")
	fetched := false
	upd := update.NewCompleted(updateID, func(context.Context) (*updatepb.Outcome, error) {
		fetched = true
		return outcome, nil
	})
	require.False(t, fetched)
	acceptedOutcome, err := upd.WaitAccepted(context.TODO())
	require.NoError(t, err)
	require.Equal(t, outcome, acceptedOutcome)

	got, err := upd.WaitOutcome(context.TODO())
	require.True(t, fetched)
	require.NoError(t, err)
	require.Equal(t, outcome, got)
}

func TestDuplicateRequestNoError(t *testing.T) {
	t.Parallel()
	updateID := t.Name() + "-update-id"
	upd := update.NewAccepted(updateID, ignoreCompletion)
	err := upd.OnMessage(context.TODO(), &updatepb.Request{}, eventStoreUnused)
	require.NoError(t, err,
		"a second request message should be ignored, not cause an error")

	msgs := make([]*protocolpb.Message, 0)
	upd.PollOutboundMessages(&msgs)
	require.Empty(t, msgs)
}

func TestMessageValidation(t *testing.T) {
	t.Parallel()
	var invalidArg *serviceerror.InvalidArgument
	updateID := t.Name() + "-update-id"
	t.Run("invalid request msg", func(t *testing.T) {
		upd := update.New("", ignoreCompletion)
		err := upd.OnMessage(context.TODO(), &updatepb.Request{}, eventStoreUnused)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
	t.Run("invalid acceptance msg", func(t *testing.T) {
		upd := update.New(updateID, ignoreCompletion)
		store := mockEventStore{Controller: effect.Immediate(context.TODO())}
		validReq := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not empty"},
		}
		err := upd.OnMessage(context.TODO(), &validReq, store)
		require.NoError(t, err)
		err = upd.OnMessage(context.TODO(), &updatepb.Acceptance{}, store)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
	t.Run("invalid rejection msg", func(t *testing.T) {
		upd := update.New(updateID, ignoreCompletion)
		store := mockEventStore{Controller: effect.Immediate(context.TODO())}
		validReq := updatepb.Request{
			Meta:  &updatepb.Meta{UpdateId: updateID},
			Input: &updatepb.Input{Name: "not empty"},
		}
		err := upd.OnMessage(context.TODO(), &validReq, store)
		require.NoError(t, err)
		err = upd.OnMessage(context.TODO(), &updatepb.Rejection{}, store)
		require.ErrorAs(t, err, &invalidArg)
		require.ErrorContains(t, err, "invalid")
	})
	t.Run("invalid response msg", func(t *testing.T) {
		upd := update.NewAccepted("", ignoreCompletion)
		err := upd.OnMessage(context.TODO(), &updatepb.Response{}, eventStoreUnused)
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
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		reqMsgID  = t.Name() + "-req-msg-id"
		meta      = updatepb.Meta{UpdateId: t.Name() + "-update-id"}
		req       = updatepb.Request{Meta: &meta, Input: &updatepb.Input{Name: t.Name()}}
		acpt      = updatepb.Acceptance{AcceptedRequest: &req, AcceptedRequestMessageId: reqMsgID}
		resp      = updatepb.Response{Meta: &meta, Outcome: successOutcome(t, "success!")}
	)

	upd := update.New(meta.UpdateId, func() { completed = true })
	require.NoError(t, upd.OnMessage(context.TODO(), &req, store))
	effects.Apply(context.TODO())

	require.NoError(t, upd.OnMessage(context.TODO(), &acpt, store))
	require.NoError(t, upd.OnMessage(context.TODO(), &resp, store))
	require.False(t, completed)

	// pretend MutalbeState write to DB fails - unwind effects
	effects.Cancel(context.TODO())

	t.Run("not accepted", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err := upd.WaitAccepted(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err := upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("back to requested state", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &acpt, store)
		require.NoError(t, err, "update should be back in Requested state")
	})
}

func TestRollbackCompletion(t *testing.T) {
	t.Parallel()
	var (
		completed = false
		effects   = effect.Buffer{}
		store     = mockEventStore{Controller: &effects}
		updateID  = t.Name() + "-update-id"
		upd       = update.NewAccepted(updateID, func() { completed = true })
		resp      = updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: updateID},
			Outcome: successOutcome(t, "success!"),
		}
	)

	require.NoError(t, upd.OnMessage(context.TODO(), &resp, store))
	require.False(t, completed)

	// pretend MutalbeState write to DB fails - unwind effects
	effects.Cancel(context.TODO())

	t.Run("not completed", func(t *testing.T) {
		ctx, cncl := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cncl()
		_, err := upd.WaitOutcome(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, completed)
	})
	t.Run("back to accepted state", func(t *testing.T) {
		err := upd.OnMessage(context.TODO(), &resp, store)
		require.NoError(t, err, "update should be back in Accepted state")
	})
}

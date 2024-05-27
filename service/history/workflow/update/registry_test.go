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

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"

	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	. "go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

func TestFind(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	t.Run("return update when found in registry", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
				return &updatepb.Outcome{
					Value: &updatepb.Outcome_Success{Success: tv.Any().Payloads()},
				}, nil
			},
		})
		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)
	})

	t.Run("return update when found in store", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				storedUpdate := &persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Acceptance{
						Acceptance: &persistencespb.UpdateAcceptanceInfo{},
					},
				}
				visitor(tv.UpdateID(), storedUpdate)
			},
		})
		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)
	})

	t.Run("return nil when not found", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		upd := reg.Find(context.Background(), tv.UpdateID())
		require.Nil(t, upd)
	})

	t.Run("return completed update with error when loading from store fails", func(t *testing.T) {
		internalErr := serviceerror.NewInternal("internal error")

		reg := update.NewRegistry(&mockUpdateStore{
			GetUpdateOutcomeFunc: func(context.Context, string) (*updatepb.Outcome, error) {
				return nil, internalErr
			},
		})
		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		_, err := upd.WaitLifecycleStage(
			context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
		require.Equal(t, internalErr, err)
	})
}

func TestFindOrCreate(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	t.Run("find stored update", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				storedUpdate := &persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Acceptance{
						Acceptance: &persistencespb.UpdateAcceptanceInfo{},
					},
				}
				visitor(tv.UpdateID(), storedUpdate)
			},
		})

		upd, found, err := reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, upd)
	})

	t.Run("create update if not found", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)

		upd, found, err := reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)
		require.False(t, found)
		require.NotNil(t, upd)

		upd, found, err = reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)
		require.True(t, found, "second lookup for same updateID should find previous")
		require.NotNil(t, upd)
	})

	t.Run("enforce in-flight update limit", func(t *testing.T) {
		var (
			limit = 1
			reg   = update.NewRegistry(
				emptyUpdateStore,
				update.WithInFlightLimit(
					func() int { return limit },
				),
			)
			evStore = mockEventStore{Controller: effect.Immediate(context.Background())}
		)

		// create an in-flight update #1
		upd1, existed, err := reg.FindOrCreate(context.Background(), tv.UpdateID("1"))
		require.NoError(t, err, "creating update #1 should have beeen allowed")
		require.False(t, existed)
		require.Equal(t, 1, reg.Len())

		t.Run("deny new update since it is exceeding the limit", func(t *testing.T) {
			_, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("admitting 1st update still denies new update to be created", func(t *testing.T) {
			err = upd1.Admit(
				context.Background(),
				&updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
					Input: &updatepb.Input{Name: "not_empty"},
				},
				evStore)
			require.NoError(t, err, "update #1 should be admitted")

			_, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("sending 1st update still denies new update to be created", func(t *testing.T) {
			_ = upd1.Send(
				context.Background(), false, &protocolpb.Message_EventId{EventId: testSequencingEventID})

			_, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("increasing limit allows new updated to be created", func(t *testing.T) {
			_, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh)
			require.Equal(t, 1, reg.Len())

			limit += 1

			_, existed, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			require.NoError(t, err, "creating update #2 should have beeen created after limit increase")
			require.False(t, existed)
			require.Equal(t, 2, reg.Len())
		})

		t.Run("rejecting 1st update allows new update to be created", func(t *testing.T) {
			rej := protocolpb.Message{Body: MarshalAny(t, &updatepb.Rejection{
				RejectedRequestMessageId: "update1/request",
				RejectedRequest: &updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
					Input: &updatepb.Input{Name: "not_empty"},
				},
				Failure: &failurepb.Failure{Message: "intentional failure in " + t.Name()},
			})}
			require.NoError(t, upd1.OnProtocolMessage(context.Background(), &rej, evStore))
			require.Equal(t, 1, reg.Len(), "update #1 should be removed from registry")

			_, existed, err = reg.FindOrCreate(context.Background(), tv.UpdateID("3"))
			require.NoError(t, err, "update #3 should be created after #1 completed")
			require.False(t, existed)
			require.Equal(t, 2, reg.Len())
		})
	})

	t.Run("enforce total update limit", func(t *testing.T) {
		var (
			limit = 1
			reg   = update.NewRegistry(
				emptyUpdateStore,
				update.WithTotalLimit(
					func() int { return limit },
				),
			)
			evStore = mockEventStore{Controller: effect.Immediate(context.Background())}
		)

		// create an in-flight update #1
		upd1, existed, err := reg.FindOrCreate(context.Background(), tv.UpdateID("1"))
		require.NoError(t, err, "creating update #1 should have beeen allowed")
		require.False(t, existed)
		require.Equal(t, 1, reg.Len())

		t.Run("deny new update since it is exceeding the limit", func(t *testing.T) {
			_, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var failedPrecon *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecon)
			require.Equal(t, 1, reg.Len())
		})

		t.Run("completing 1st update still denies new update to be created", func(t *testing.T) {
			err = upd1.Admit(
				context.Background(),
				&updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
					Input: &updatepb.Input{Name: "not_empty"},
				},
				evStore)
			require.NoError(t, err, "update #1 should be admitted")

			err = upd1.OnProtocolMessage(
				context.Background(),
				&protocolpb.Message{Body: MarshalAny(t, &updatepb.Rejection{
					RejectedRequestMessageId: "update1/request",
					RejectedRequest: &updatepb.Request{
						Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
						Input: &updatepb.Input{Name: "not_empty"},
					},
					Failure: &failurepb.Failure{
						Message: "intentional failure in " + t.Name(),
					},
				})},
				evStore)
			require.NoError(t, err, "update #1 should be completed")

			_, existed, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			var failedPrecon *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecon)
			require.Equal(t, 0, reg.Len())
		})

		t.Run("increasing limit allows new updated to be created", func(t *testing.T) {
			limit = 2

			_, existed, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
			require.NoError(t, err, "update #2 should be created after the limit increase")
			require.False(t, existed)
			require.Equal(t, 1, reg.Len())
		})
	})
}

func TestHasOutgoingMessages(t *testing.T) {
	t.Parallel()

	var (
		tv      = testvars.New(t.Name())
		upd     *update.Update
		reg     = update.NewRegistry(emptyUpdateStore)
		evStore = mockEventStore{Controller: effect.Immediate(context.Background())}
	)

	t.Run("empty registry", func(t *testing.T) {
		require.False(t, reg.HasOutgoingMessages(false))
	})

	t.Run("registry with created update", func(t *testing.T) {
		var err error
		upd, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)

		require.False(t, reg.HasOutgoingMessages(false))
	})

	t.Run("registy with admitted update", func(t *testing.T) {
		err := upd.Admit(
			context.Background(),
			&updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID()},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			evStore)
		require.NoError(t, err)

		require.True(t, reg.HasOutgoingMessages(false))
		require.True(t, reg.HasOutgoingMessages(true))
	})

	t.Run("registry with sent update", func(t *testing.T) {
		msg := reg.Send(context.Background(), false, testSequencingEventID)
		require.Len(t, msg, 1)

		require.False(t, reg.HasOutgoingMessages(false))
		require.True(t, reg.HasOutgoingMessages(true))
	})

	t.Run("registry with accepted update", func(t *testing.T) {
		err := upd.OnProtocolMessage(
			context.Background(),
			&protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
				AcceptedRequestMessageId:         tv.MessageID(),
				AcceptedRequestSequencingEventId: testSequencingEventID,
			})},
			evStore)
		require.NoError(t, err)

		require.False(t, reg.HasOutgoingMessages(false))
		require.False(t, reg.HasOutgoingMessages(true))
	})
}

func TestUpdateAccepted_WorkflowCompleted(t *testing.T) {
	t.Parallel()
	var (
		ctx                    = context.Background()
		storedAcceptedUpdateID = t.Name() + "-accepted-update-id"
		regStore               = mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				storedAcceptedUpdateInfo := &persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Acceptance{
						Acceptance: &persistencespb.UpdateAcceptanceInfo{
							EventId: 22,
						},
					},
				}
				visitor(storedAcceptedUpdateID, storedAcceptedUpdateInfo)
			},
			IsWorkflowExecutionRunningFunc: func() bool {
				return false
			},
		}
		reg     = update.NewRegistry(regStore)
		effects = effect.Buffer{}
		evStore = mockEventStore{Controller: &effects}
	)

	upd, found, err := reg.FindOrCreate(ctx, storedAcceptedUpdateID)
	require.NoError(t, err)
	require.True(t, found)

	// Even timeout is very short, it won't fire but error will be returned right away.
	s, err := upd.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 100*time.Millisecond)
	require.Error(t, err)
	require.Nil(t, s)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "workflow execution already completed", err.Error())

	meta := updatepb.Meta{UpdateId: storedAcceptedUpdateID}
	outcome := successOutcome(t, "success!")
	err = upd.OnProtocolMessage(
		ctx,
		&protocolpb.Message{Body: MarshalAny(t, &updatepb.Response{Meta: &meta, Outcome: outcome})},
		evStore,
	)
	require.Error(t, err, "should not be able to completed update for completed workflow")
	require.Equal(t, 1, reg.Len(), "update should still be present in map")
}

func TestSendMessages(t *testing.T) {
	t.Parallel()

	var (
		tv         = testvars.New(t.Name())
		upd1, upd2 *update.Update
		reg        = update.NewRegistry(emptyUpdateStore)
		evStore    = mockEventStore{Controller: effect.Immediate(context.Background())}
	)

	t.Run("empty registry has no messages to send", func(t *testing.T) {
		msgs := reg.Send(context.Background(), true, testSequencingEventID)
		require.Empty(t, msgs)
	})

	t.Run("registry with 2 created updates has no messages to send", func(t *testing.T) {
		var err error
		upd1, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("1"))
		require.NoError(t, err)
		upd2, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
		require.NoError(t, err)

		msgs := reg.Send(context.Background(), true, testSequencingEventID)
		require.Empty(t, msgs)
		require.False(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
	})

	t.Run("registy with 1 admitted update has 1 message to send", func(t *testing.T) {
		err := upd1.Admit(
			context.Background(),
			&updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			evStore)
		require.NoError(t, err)

		msgs := reg.Send(context.Background(), false, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
		require.Equal(t, tv.UpdateID("1"), msgs[0].ProtocolInstanceId)
		require.Equal(t, testSequencingEventID-1, msgs[0].GetEventId())

		// no more to send as update #1 is already sent
		msgs = reg.Send(context.Background(), false, testSequencingEventID)
		require.Empty(t, msgs)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())

		// including already sent updates returns update #1 message again
		msgs = reg.Send(context.Background(), true, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
	})

	t.Run("registy with 2 admitted updates returns messages sorted by admission time", func(t *testing.T) {
		err := upd2.Admit(
			context.Background(),
			&updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("2")},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			evStore)
		require.NoError(t, err)

		msgs := reg.Send(context.Background(), false, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())

		// no more to send as update #1 and #2 are already sent
		msgs = reg.Send(context.Background(), false, testSequencingEventID)
		require.Empty(t, msgs)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())

		// including already sent updates returns update #1 and #2 message again
		msgs = reg.Send(context.Background(), true, testSequencingEventID)
		require.Len(t, msgs, 2)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())
		require.Equal(t, tv.UpdateID("1"), msgs[0].ProtocolInstanceId)
		require.Equal(t, tv.UpdateID("2"), msgs[1].ProtocolInstanceId)
	})
}

func TestRejectUnprocessed(t *testing.T) {
	t.Parallel()

	var (
		tv           = testvars.New(t.Name())
		upd1, upd2   *update.Update
		reg          = update.NewRegistry(emptyUpdateStore)
		evStore      = mockEventStore{Controller: effect.Immediate(context.Background())}
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)

	t.Run("empty registry has no updates to reject", func(t *testing.T) {
		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with updates [#1, #2] in stateCreated rejects nothing", func(t *testing.T) {
		var err error
		upd1, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("1"))
		require.NoError(t, err)
		upd2, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID("2"))
		require.NoError(t, err)

		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with updates [#1, #2] in stateAdmitted rejects nothing", func(t *testing.T) {
		err := upd1.Admit(
			context.Background(),
			&updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			evStore)
		require.NoError(t, err)
		err = upd2.Admit(
			context.Background(),
			&updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("2")},
				Input: &updatepb.Input{Name: "not_empty"},
			},
			evStore)
		require.NoError(t, err)

		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with update #1 in stateSent rejects it", func(t *testing.T) {
		_ = upd1.Send(context.Background(), false, sequencingID)

		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Len(t, rejectedIDs, 1, "only update #1 in stateSent should be rejected")
		require.Equal(t, rejectedIDs[0], tv.UpdateID("1"), "update #1 should be rejected")

		rejectedIDs, err = reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs, "rejected update #1 should not be rejected again")
	})

	t.Run("registry with update #2 in stateAccepted rejects nothing", func(t *testing.T) {
		_ = upd2.Send(context.Background(), false, sequencingID)
		err := upd2.OnProtocolMessage(
			context.Background(),
			&protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
				AcceptedRequestMessageId:         tv.String(),
				AcceptedRequestSequencingEventId: testSequencingEventID,
			})},
			evStore)
		require.NoError(t, err)

		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with update #2 in stateCompleted rejects nothing", func(t *testing.T) {
		err := upd2.OnProtocolMessage(
			context.Background(),
			&protocolpb.Message{Body: MarshalAny(t, &updatepb.Response{
				Meta:    &updatepb.Meta{UpdateId: tv.UpdateID("2")},
				Outcome: successOutcome(t, tv.String())}),
			},
			evStore)
		require.NoError(t, err)

		rejectedIDs, err := reg.RejectUnprocessed(context.Background(), evStore)
		require.NoError(t, err)
		require.Empty(t, rejectedIDs)
	})
}

func TestAbort(t *testing.T) {
	var (
		ctx          = context.Background()
		evStore      = mockEventStore{Controller: effect.Immediate(ctx)}
		reg          = update.NewRegistry(emptyUpdateStore)
		sequencingID = &protocolpb.Message_EventId{EventId: testSequencingEventID}
	)
	updateID1, updateID2, updateID3, updateID4, updateID5 := t.Name()+"-update-id-1", t.Name()+"-update-id-2", t.Name()+"-update-id-3", t.Name()+"-update-id-4", t.Name()+"-update-id-5"
	updCreated, _, _ := reg.FindOrCreate(ctx, updateID1)

	updAdmitted, _, _ := reg.FindOrCreate(ctx, updateID2)
	_ = updAdmitted.Admit(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID2},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)

	updSent, _, _ := reg.FindOrCreate(ctx, updateID3)
	_ = updSent.Admit(ctx, &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID3},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}, evStore)
	updSent.Send(ctx, false, sequencingID)

	msgRequest4 := &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID4},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}
	updAccepted, _, _ := reg.FindOrCreate(ctx, updateID4)
	_ = updAccepted.Admit(ctx, msgRequest4, evStore)
	updAccepted.Send(ctx, false, sequencingID)
	_ = updAccepted.OnProtocolMessage(ctx, &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
		AcceptedRequestMessageId:         "random",
		AcceptedRequestSequencingEventId: testSequencingEventID,
		AcceptedRequest:                  msgRequest4,
	})}, evStore)

	msgRequest5 := &updatepb.Request{
		Meta:  &updatepb.Meta{UpdateId: updateID5},
		Input: &updatepb.Input{Name: t.Name() + "-update-func"},
	}
	updCompleted, _, _ := reg.FindOrCreate(ctx, updateID5)
	_ = updCompleted.Admit(ctx, msgRequest5, evStore)
	updCompleted.Send(ctx, false, sequencingID)
	_ = updCompleted.OnProtocolMessage(ctx, &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
		AcceptedRequestMessageId:         "random",
		AcceptedRequestSequencingEventId: testSequencingEventID,
		AcceptedRequest:                  msgRequest4,
	})}, evStore)
	_ = updCompleted.OnProtocolMessage(
		ctx,
		&protocolpb.Message{Body: MarshalAny(t, &updatepb.Response{Meta: &updatepb.Meta{UpdateId: updateID5}, Outcome: successOutcome(t, "update completed")})},
		evStore)

	reg.Abort(update.AbortReasonWorkflowCompleted)

	status, err := updCreated.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
	require.Nil(t, status)

	status, err = updAdmitted.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
	require.Nil(t, status)

	status, err = updSent.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
	require.Nil(t, status)

	status, err = updAccepted.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
	require.Nil(t, status)

	status, err = updCompleted.WaitLifecycleStage(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, 1*time.Second)
	require.NoError(t, err)
	require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, status.Stage)
	require.Nil(t, status.Outcome.GetFailure())
	require.NotNil(t, status.Outcome.GetSuccess())
}

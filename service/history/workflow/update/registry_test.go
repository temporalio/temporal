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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/types/known/anypb"

	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	. "go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/workflow/update"
)

func TestNewRegistry(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	t.Run("registry created from empty store has no updates", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)

		require.Empty(t, reg.Len())
		require.False(t, reg.Contains(tv.UpdateID()))
	})

	t.Run("registry created from store with update in stateAdmitted contains admitted update", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				visitor(
					tv.UpdateID(),
					&persistencespb.UpdateInfo{
						Value: &persistencespb.UpdateInfo_Admission{
							Admission: &persistencespb.UpdateAdmissionInfo{},
						},
					})
			},
		})

		require.Equal(t, 1, reg.Len())
		require.True(t, reg.Contains(tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)
	})

	t.Run("registry created from store with update in stateAccepted contains accepted update", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				visitor(
					tv.UpdateID(),
					&persistencespb.UpdateInfo{
						Value: &persistencespb.UpdateInfo_Acceptance{
							Acceptance: &persistencespb.UpdateAcceptanceInfo{},
						},
					})
			},
		})

		require.Equal(t, 1, reg.Len())
		require.True(t, reg.Contains(tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, s.Stage)
	})

	t.Run("registry created from store with update in stateAccepted but non-running workflow contains aborted update", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				visitor(
					tv.UpdateID(),
					&persistencespb.UpdateInfo{
						Value: &persistencespb.UpdateInfo_Acceptance{
							Acceptance: &persistencespb.UpdateAcceptanceInfo{},
						},
					})
			},
			IsWorkflowExecutionRunningFunc: func() bool { return false },
		})

		require.Equal(t, 1, reg.Len())
		require.True(t, reg.Contains(tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		_, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.Equal(t, err, consts.ErrWorkflowCompleted)
	})

	t.Run("registry created from store with update in stateCompleted has no updates but increased completed count", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
				visitor(
					tv.UpdateID(),
					&persistencespb.UpdateInfo{
						Value: &persistencespb.UpdateInfo_Completion{
							Completion: &persistencespb.UpdateCompletionInfo{},
						},
					})
			},
		})

		require.Equal(t, 0, reg.Len())
		require.Equal(t, 1, update.CompletedCount(reg))
	})
}

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

	t.Run("registry with admitted update", func(t *testing.T) {
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

	t.Run("registry with 1 admitted update has 1 message to send", func(t *testing.T) {
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

	t.Run("registry with 2 admitted updates returns messages sorted by admission time", func(t *testing.T) {
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

// NOTE: tests for various update states can be found in the update tests
func TestAbort(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	reg := update.NewRegistry(&mockUpdateStore{
		VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
			visitor(
				tv.UpdateID("1"),
				&persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Admission{
						Admission: &persistencespb.UpdateAdmissionInfo{},
					},
				})
			visitor(
				tv.UpdateID("2"),
				&persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Admission{
						Admission: &persistencespb.UpdateAdmissionInfo{},
					},
				})
		},
	})

	// both updates are aborted now
	var wg sync.WaitGroup
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			upd := reg.Find(context.Background(), tv.UpdateID(fmt.Sprintf("%d", id)))
			require.NotNil(t, upd)

			_, err := upd.WaitLifecycleStage(context.Background(), 0, 1*time.Second)
			require.Equal(t, consts.ErrWorkflowCompleted, err)
		}(i)
	}

	reg.Abort(update.AbortReasonWorkflowCompleted)
	wg.Wait()

	require.Equal(t, 2, reg.Len(), "registry should still contain both updates")
}

func TestClear(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	reg := update.NewRegistry(&mockUpdateStore{
		VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
			visitor(
				tv.UpdateID(),
				&persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Admission{
						Admission: &persistencespb.UpdateAdmissionInfo{},
					},
				})
		},
	})

	upd := reg.Find(context.Background(), tv.UpdateID())
	require.NotNil(t, upd)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := upd.WaitLifecycleStage(context.Background(), 0, 1*time.Second)
		require.Equal(t, update.WorkflowUpdateAbortedErr, err)
	}()

	reg.Clear()
	wg.Wait()

	require.Equal(t, reg.Len(), 0, "registry should be cleared")
}

func TestFailoverVersion(t *testing.T) {
	t.Parallel()

	t.Run("return version obtained from store", func(t *testing.T) {
		reg := update.NewRegistry(&mockUpdateStore{
			GetCurrentVersionFunc: func() int64 { return 42 },
		})
		require.Equal(t, int64(42), reg.FailoverVersion())
	})

	t.Run("returns same version obtained from store after store changes its version", func(t *testing.T) {
		var failoverVersion int64 = 42
		reg := update.NewRegistry(&mockUpdateStore{
			GetCurrentVersionFunc: func() int64 { return failoverVersion },
		})
		failoverVersion = 1024

		require.Equal(t, int64(42), reg.FailoverVersion(),
			"should still be original failover version")
	})
}

func TestTryResurrect(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t.Name())

	t.Run("add acceptance message as new update with stateAdmitted", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
			AcceptedRequest:                  &updatepb.Request{},
		})}

		upd, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)
	})

	t.Run("add rejection message as new update with stateAdmitted", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Rejection{
			RejectedRequestMessageId:         tv.MessageID(),
			RejectedRequestSequencingEventId: testSequencingEventID,
			RejectedRequest:                  &updatepb.Request{},
		})}

		upd, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)
	})

	t.Run("ignore nil messages", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)

		upd, err := reg.TryResurrect(context.Background(), nil)
		require.Nil(t, err)
		require.Nil(t, upd)

		upd, err = reg.TryResurrect(context.Background(), &protocolpb.Message{Body: nil})
		require.Nil(t, err)
		require.Nil(t, upd)
	})

	t.Run("ignore completed protocol message", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		completedMsg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Outcome{
			Value: &updatepb.Outcome_Success{Success: tv.Any().Payloads()},
		})}

		upd, err := reg.TryResurrect(context.Background(), completedMsg)
		require.Nil(t, err)
		require.Nil(t, upd)
	})

	t.Run("ignore invalid message body", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		invalidMsg := &protocolpb.Message{Body: &anypb.Any{TypeUrl: "invalid"}}

		_, err := reg.TryResurrect(context.Background(), invalidMsg)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
		require.Equal(t, 0, reg.Len())
	})

	t.Run("do not enforce in-flight update limit", func(t *testing.T) {
		reg := update.NewRegistry(
			emptyUpdateStore,
			update.WithInFlightLimit(
				func() int { return 0 },
			),
		)
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}

		_, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
	})

	t.Run("enforce total update limit", func(t *testing.T) {
		reg := update.NewRegistry(
			emptyUpdateStore,
			update.WithTotalLimit(
				func() int { return 0 },
			),
		)
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}

		_, err := reg.TryResurrect(context.Background(), msg)
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon)
		require.Equal(t, 0, reg.Len())
	})
}

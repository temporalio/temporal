package update_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/effect"
	. "go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNewRegistry(t *testing.T) {
	tv := testvars.New(t)

	t.Run("registry created from empty store has no updates", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)

		require.Empty(t, reg.Len())
		require.Nil(t, reg.Find(context.Background(), tv.UpdateID()))
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
		evStore := mockEventStore{Controller: effect.Immediate(context.Background())}

		require.Equal(t, 1, reg.Len())
		require.NotNil(t, reg.Find(context.Background(), tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)

		// ensure update can complete its lifecycle
		mustAccept(t, evStore, upd)
		assertCompleteUpdateInRegistry(t, reg, evStore, upd)
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
		evStore := mockEventStore{Controller: effect.Immediate(context.Background())}

		require.Equal(t, 1, reg.Len())
		require.NotNil(t, reg.Find(context.Background(), tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, s.Stage)

		// ensure update can complete its lifecycle
		assertCompleteUpdateInRegistry(t, reg, evStore, upd)
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
		require.NotNil(t, reg.Find(context.Background(), tv.UpdateID()))

		upd := reg.Find(context.Background(), tv.UpdateID())
		require.NotNil(t, upd)

		status, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, status)
		require.Equal(t, "Workflow Update failed because the Workflow completed before the Update completed.", status.Outcome.GetFailure().Message)
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
	tv := testvars.New(t)

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
	tv := testvars.New(t)
	tv1 := tv.WithUpdateIDNumber(1)
	tv2 := tv.WithUpdateIDNumber(2)
	tv3 := tv.WithUpdateIDNumber(3)
	tv4 := tv.WithUpdateIDNumber(4)

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
		evStore := mockEventStore{Controller: effect.Immediate(context.Background())}

		upd, found, err := reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)
		require.False(t, found)
		require.NotNil(t, upd)

		upd, found, err = reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)
		require.True(t, found, "second lookup for same updateID should find previous")
		require.NotNil(t, upd)

		// ensure update can complete its lifecycle
		mustAdmit(t, evStore, upd)
		mustAccept(t, evStore, upd)
		assertCompleteUpdateInRegistry(t, reg, evStore, upd)
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
		upd1, existed, err := reg.FindOrCreate(context.Background(), tv1.UpdateID())
		require.NoError(t, err, "creating update #1 should have been allowed")
		require.False(t, existed)
		require.Equal(t, 1, reg.Len())

		t.Run("deny new update since it is exceeding the limit", func(t *testing.T) {
			_, _, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("admitting 1st update still denies new update to be created", func(t *testing.T) {
			mustAdmit(t, evStore, upd1)

			_, _, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("sending 1st update still denies new update to be created", func(t *testing.T) {
			require.NotNil(t, send(t, upd1, includeAlreadySent), "update should be sent")

			_, _, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "creating update #2 should be denied")
			require.Equal(t, 1, reg.Len())
		})

		t.Run("increasing limit allows new updated to be created", func(t *testing.T) {
			_, _, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh)
			require.Equal(t, 1, reg.Len())

			limit += 1

			_, existed, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			require.NoError(t, err, "update #2 should have beeen created after limit increase")
			require.False(t, existed)
			require.Equal(t, 2, reg.Len())
		})

		t.Run("rejecting 1st update allows new update to be created", func(t *testing.T) {
			assertRejectUpdateInRegistry(t, reg, evStore, upd1)

			_, existed, err = reg.FindOrCreate(context.Background(), tv3.UpdateID())
			require.NoError(t, err, "update #3 should have been created after #1 completed")
			require.False(t, existed)
			require.Equal(t, 2, reg.Len())
		})

		t.Run("disable limit by setting it to zero", func(t *testing.T) {
			limit = 0

			_, _, err = reg.FindOrCreate(context.Background(), tv4.UpdateID())
			require.NoError(t, err, "update #4 should have been created")
			require.False(t, existed)
			require.Equal(t, 3, reg.Len())
		})
	})

	t.Run("enforce total update limit", func(t *testing.T) {
		var limit = 1

		newRegistryWithSingleInflightUpdate := func() (update.Registry, mockEventStore, *update.Update) {
			t.Helper()

			reg := update.NewRegistry(
				emptyUpdateStore,
				update.WithTotalLimit(
					func() int { return limit },
				),
			)

			// create an in-flight update #1
			upd1, existed, err := reg.FindOrCreate(context.Background(), tv1.UpdateID())
			require.NoError(t, err, "creating update #1 should have been allowed")
			require.False(t, existed)
			require.Equal(t, 1, reg.Len())

			evStore := mockEventStore{Controller: effect.Immediate(context.Background())}
			mustAdmit(t, evStore, upd1)
			require.Equal(t, 1, reg.Len())

			return reg, evStore, upd1
		}

		t.Run("deny new update since it is exceeding the limit", func(t *testing.T) {
			reg, _, _ := newRegistryWithSingleInflightUpdate()

			_, _, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var failedPrecon *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecon)
			require.Equal(t, 1, reg.Len())
		})

		t.Run("rejecting 1st update now allows new update to be created", func(t *testing.T) {
			reg, evStore, upd1 := newRegistryWithSingleInflightUpdate()
			assertRejectUpdateInRegistry(t, reg, evStore, upd1)
			require.Equal(t, 0, reg.Len())

			_, existed, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
			require.NoError(t, err)
			require.False(t, existed)
			require.Equal(t, 1, reg.Len())
		})

		t.Run("accepting 1st update still denies new update to be created", func(t *testing.T) {
			reg, evStore, upd1 := newRegistryWithSingleInflightUpdate()
			mustAccept(t, evStore, upd1)

			_, _, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var failedPrecon *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecon)
			require.Equal(t, 1, reg.Len())
		})

		t.Run("completing 1st update still denies new update to be created", func(t *testing.T) {
			reg, evStore, upd1 := newRegistryWithSingleInflightUpdate()
			mustAccept(t, evStore, upd1)
			assertCompleteUpdateInRegistry(t, reg, evStore, upd1)
			require.Equal(t, 0, reg.Len())

			_, _, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
			var failedPrecon *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecon)
			require.Equal(t, 0, reg.Len())
		})

		t.Run("increasing limit allows new updated to be created", func(t *testing.T) {
			reg, _, _ := newRegistryWithSingleInflightUpdate()
			limit = 2

			_, existed, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
			require.NoError(t, err)
			require.False(t, existed)
			require.Equal(t, 2, reg.Len())
		})
	})

	t.Run("enforce inflight update size limit", func(t *testing.T) {
		var (
			updateSize = 300
			reg        = update.NewRegistry(
				emptyUpdateStore,
				update.WithInFlightSizeLimit(
					func() int { return updateSize },
				),
			)
			evStore = mockEventStore{Controller: effect.Immediate(context.Background())}
		)

		// create an in-flight updates
		upd1, existed, err := reg.FindOrCreate(context.Background(), tv1.UpdateID())
		require.NoError(t, err)
		require.False(t, existed)

		upd2, existed, err := reg.FindOrCreate(context.Background(), tv2.UpdateID())
		require.NoError(t, err)
		require.False(t, existed)

		upd3, existed, err := reg.FindOrCreate(context.Background(), tv3.UpdateID())
		require.NoError(t, err)
		require.False(t, existed)

		upd4, existed, err := reg.FindOrCreate(context.Background(), tv4.UpdateID())
		require.NoError(t, err)
		require.False(t, existed)

		t.Run("admitting update #1 is allowed", func(t *testing.T) {
			mustAdmit(t, evStore, upd1)
		})

		t.Run("rejecting update #1 allows update #2 to be admitted", func(t *testing.T) {
			err = admit(t, evStore, upd2)
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "admitting update #2 should be denied")

			assertRejectUpdateInRegistry(t, reg, evStore, upd1)

			_, existed, err = reg.FindOrCreate(context.Background(), tv2.UpdateID())
			require.NoError(t, err, "update #2 should have been admitted")
			require.False(t, existed)
		})

		t.Run("increasing limit allows update #3 to be admitted", func(t *testing.T) {
			updateSize = 1

			// at first, it's denied
			err = admit(t, evStore, upd3)
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "admitting update #3 should be denied")

			updateSize = 1024

			mustAdmit(t, evStore, upd3)
		})

		t.Run("disabling limit allows update #4 to be admitted", func(t *testing.T) {
			updateSize = 1

			// at first, it's denied
			err = admit(t, evStore, upd4)
			var resExh *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resExh, "admitting update #4 should be denied")

			updateSize = 0

			mustAdmit(t, evStore, upd4)
		})
	})
}

func TestHasOutgoingMessages(t *testing.T) {
	t.Parallel()

	var (
		tv      = testvars.New(t)
		upd     *update.Update
		reg     = update.NewRegistry(emptyUpdateStore)
		evStore = mockEventStore{Controller: effect.Immediate(context.Background())}
	)

	t.Run("empty registry", func(t *testing.T) {
		require.False(t, reg.HasOutgoingMessages(skipAlreadySent))
	})

	t.Run("registry with created update", func(t *testing.T) {
		var err error
		upd, _, err = reg.FindOrCreate(context.Background(), tv.UpdateID())
		require.NoError(t, err)

		require.False(t, reg.HasOutgoingMessages(skipAlreadySent))
	})

	t.Run("registry with admitted update", func(t *testing.T) {
		mustAdmit(t, evStore, upd)

		require.True(t, reg.HasOutgoingMessages(skipAlreadySent))
		require.True(t, reg.HasOutgoingMessages(includeAlreadySent))
	})

	t.Run("registry with sent update", func(t *testing.T) {
		msg := reg.Send(context.Background(), skipAlreadySent, testSequencingEventID)
		require.Len(t, msg, 1)

		require.False(t, reg.HasOutgoingMessages(skipAlreadySent))
		require.True(t, reg.HasOutgoingMessages(includeAlreadySent))
	})

	t.Run("registry with accepted update", func(t *testing.T) {
		mustAccept(t, evStore, upd)

		require.False(t, reg.HasOutgoingMessages(skipAlreadySent))
		require.False(t, reg.HasOutgoingMessages(includeAlreadySent))
	})
}

func TestSendMessages(t *testing.T) {
	t.Parallel()

	var (
		tv         = testvars.New(t)
		upd1, upd2 *update.Update
		reg        = update.NewRegistry(emptyUpdateStore)
		evStore    = mockEventStore{Controller: effect.Immediate(context.Background())}
	)

	t.Run("empty registry has no messages to send", func(t *testing.T) {
		msgs := reg.Send(context.Background(), includeAlreadySent, testSequencingEventID)
		require.Empty(t, msgs)
	})

	t.Run("registry with 2 created updates has no messages to send", func(t *testing.T) {
		var err error
		upd1, _, err = reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(1).UpdateID())
		require.NoError(t, err)
		upd2, _, err = reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(2).UpdateID())
		require.NoError(t, err)

		msgs := reg.Send(context.Background(), includeAlreadySent, testSequencingEventID)
		require.Empty(t, msgs)
		require.False(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
	})

	t.Run("registry with 1 admitted update has 1 message to send", func(t *testing.T) {
		mustAdmit(t, evStore, upd1)

		msgs := reg.Send(context.Background(), includeAlreadySent, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
		require.Equal(t, upd1.ID(), msgs[0].ProtocolInstanceId)
		require.Equal(t, testSequencingEventID-1, msgs[0].GetEventId())

		// no more to send as update #1 is already sent
		msgs = reg.Send(context.Background(), skipAlreadySent, testSequencingEventID)
		require.Empty(t, msgs)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())

		// including already sent updates returns update #1 message again
		msgs = reg.Send(context.Background(), includeAlreadySent, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.False(t, upd2.IsSent())
	})

	t.Run("registry with 2 admitted updates returns messages sorted by admission time", func(t *testing.T) {
		mustAdmit(t, evStore, upd2)

		msgs := reg.Send(context.Background(), skipAlreadySent, testSequencingEventID)
		require.Len(t, msgs, 1)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())

		// no more to send as update #1 and #2 are already sent
		msgs = reg.Send(context.Background(), skipAlreadySent, testSequencingEventID)
		require.Empty(t, msgs)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())

		// including already sent updates returns update #1 and #2 message again
		msgs = reg.Send(context.Background(), includeAlreadySent, testSequencingEventID)
		require.Len(t, msgs, 2)
		require.True(t, upd1.IsSent())
		require.True(t, upd2.IsSent())
		require.Equal(t, upd1.ID(), msgs[0].ProtocolInstanceId)
		require.Equal(t, upd2.ID(), msgs[1].ProtocolInstanceId)
	})
}

func TestRejectUnprocessed(t *testing.T) {
	t.Parallel()

	var (
		tv         = testvars.New(t)
		upd1, upd2 *update.Update
		reg        = update.NewRegistry(emptyUpdateStore)
		evStore    = mockEventStore{Controller: effect.Immediate(context.Background())}
	)

	t.Run("empty registry has no updates to reject", func(t *testing.T) {
		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with updates [#1, #2] in stateCreated rejects nothing", func(t *testing.T) {
		var err error
		upd1, _, err = reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(1).UpdateID())
		require.NoError(t, err)
		upd2, _, err = reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(2).UpdateID())
		require.NoError(t, err)

		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with updates [#1, #2] in stateAdmitted rejects nothing", func(t *testing.T) {
		mustAdmit(t, evStore, upd1)
		mustAdmit(t, evStore, upd2)

		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with update #1 in stateSent rejects it", func(t *testing.T) {
		t.Helper()
		require.NotNil(t, send(t, upd1, includeAlreadySent), "update should be sent")

		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Len(t, rejectedIDs, 1, "only update #1 in stateSent should be rejected")
		require.Equal(t, rejectedIDs[0], upd1.ID(), "update #1 should be rejected")

		rejectedIDs = reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs, "rejected update #1 should not be rejected again")
	})

	t.Run("registry with update #2 in stateAccepted rejects nothing", func(t *testing.T) {
		t.Helper()
		require.NotNil(t, send(t, upd2, includeAlreadySent), "update should be sent")
		mustAccept(t, evStore, upd2)

		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs)
	})

	t.Run("registry with update #2 in stateCompleted rejects nothing", func(t *testing.T) {
		t.Helper()
		require.NoError(t, respondSuccess(t, evStore, upd2), "update should be completed")
		assertCompleted(t, upd2, successOutcome)

		rejectedIDs := reg.RejectUnprocessed(context.Background(), evStore)
		require.Empty(t, rejectedIDs)
	})
}

// NOTE: tests for various update states can be found in the update tests
func TestAbort(t *testing.T) {
	tv := testvars.New(t)

	// new registry with 1 admitted update and 1 accepted update
	reg := update.NewRegistry(&mockUpdateStore{
		VisitUpdatesFunc: func(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
			visitor(
				tv.WithUpdateIDNumber(1).UpdateID(),
				&persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Admission{
						Admission: &persistencespb.UpdateAdmissionInfo{},
					},
				})
			visitor(
				tv.WithUpdateIDNumber(2).UpdateID(),
				&persistencespb.UpdateInfo{
					Value: &persistencespb.UpdateInfo_Acceptance{
						Acceptance: &persistencespb.UpdateAcceptanceInfo{},
					},
				})
		},
	})

	// abort both updates
	reg.Abort(update.AbortReasonWorkflowCompleted)

	upd1 := reg.Find(context.Background(), tv.WithUpdateIDNumber(1).UpdateID())
	require.NotNil(t, upd1)
	status1, err := upd1.WaitLifecycleStage(context.Background(), 0, 2*time.Second)
	require.Equal(t, update.AbortedByWorkflowClosingErr, err)
	require.Nil(t, status1)

	upd2 := reg.Find(context.Background(), tv.WithUpdateIDNumber(2).UpdateID())
	require.NotNil(t, upd2)
	status2, err := upd2.WaitLifecycleStage(context.Background(), 0, 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, status2)
	require.Equal(t, "Workflow Update failed because the Workflow completed before the Update completed.", status2.Outcome.GetFailure().Message)

	require.Equal(t, 2, reg.Len(), "registry should still contain both updates")
}

func TestClear(t *testing.T) {
	tv := testvars.New(t)

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
		_, err := upd.WaitLifecycleStage(
			context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, 2*time.Second)
		require.Equal(t, update.AbortedByServerErr, err)
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
	tv := testvars.New(t)

	t.Run("add acceptance message as new update with stateAdmitted", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		evStore := mockEventStore{Controller: effect.Immediate(context.Background())}
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
			AcceptedRequest:                  &updatepb.Request{},
		}), ProtocolInstanceId: tv.UpdateID()}

		upd, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)

		// ensure update can complete its lifecycle
		mustAccept(t, evStore, upd)
		assertCompleteUpdateInRegistry(t, reg, evStore, upd)
	})

	t.Run("add rejection message as new update with stateAdmitted", func(t *testing.T) {
		reg := update.NewRegistry(emptyUpdateStore)
		evStore := mockEventStore{Controller: effect.Immediate(context.Background())}
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Rejection{
			RejectedRequestMessageId:         tv.MessageID(),
			RejectedRequestSequencingEventId: testSequencingEventID,
			RejectedRequest:                  &updatepb.Request{},
		}), ProtocolInstanceId: tv.UpdateID()}

		upd, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
		require.NotNil(t, upd)

		s, err := upd.WaitLifecycleStage(context.Background(), 0, 100*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, s.Stage)

		// ensure update can complete its lifecycle
		mustAccept(t, evStore, upd)
		assertCompleteUpdateInRegistry(t, reg, evStore, upd)
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
				func() int { return 1 },
			),
		)
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId:         tv.MessageID(),
			AcceptedRequestSequencingEventId: testSequencingEventID,
		})}

		_, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
	})

	t.Run("still enforce total update limit", func(t *testing.T) {
		reg := update.NewRegistry(
			emptyUpdateStore,
			update.WithTotalLimit(
				func() int { return 1 },
			),
		)

		// 1st is allowed
		msg := &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId: "0",
			AcceptedRequest:          &updatepb.Request{},
		})}
		_, err := reg.TryResurrect(context.Background(), msg)
		require.NoError(t, err)
		require.Equal(t, 1, reg.Len())

		// 2nd is denied
		msg = &protocolpb.Message{Body: MarshalAny(t, &updatepb.Acceptance{
			AcceptedRequestMessageId: "1",
			AcceptedRequest:          &updatepb.Request{},
		})}
		_, err = reg.TryResurrect(context.Background(), msg)
		var failedPrecon *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecon)
		require.Equal(t, 1, reg.Len())
	})
}

func TestSuggestContinueAsNew(t *testing.T) {
	var (
		tv         = testvars.New(t)
		limit      = 4
		suggestCAN = 0.5
		reg        = update.NewRegistry(
			emptyUpdateStore,
			update.WithTotalLimit(func() int {
				return limit
			}),
			update.WithTotalLimitSuggestCAN(func() float64 {
				return suggestCAN
			}))
	)

	t.Run("do not suggest Continue-As-New for empty registry", func(t *testing.T) {
		require.False(t, reg.SuggestContinueAsNew())
	})

	t.Run("do not suggest Continue-As-New before limit is reached", func(t *testing.T) {
		_, existed, err := reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(1).UpdateID())
		require.NoError(t, err)
		require.False(t, existed)
		require.Equal(t, 1, reg.Len()) // ie 25% of the limit, ie below the suggestion threshold of 2

		require.False(t, reg.SuggestContinueAsNew())
	})

	t.Run("suggest Continue-As-New when limit is reached", func(t *testing.T) {
		_, existed, err := reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(2).UpdateID())
		require.NoError(t, err)
		require.False(t, existed)
		require.Equal(t, 2, reg.Len()) // ie 50% of the limit, ie at the suggestion threshold of 2

		require.True(t, reg.SuggestContinueAsNew())

		_, existed, err = reg.FindOrCreate(context.Background(), tv.WithUpdateIDNumber(3).UpdateID())
		require.NoError(t, err)
		require.False(t, existed)
		require.Equal(t, 3, reg.Len()) // ie 75% of the limit, ie above the suggestion threshold of 2

		require.True(t, reg.SuggestContinueAsNew())
	})

	t.Run("do not suggest Continue-As-New after limit is increased", func(t *testing.T) {
		limit = 10
		require.False(t, reg.SuggestContinueAsNew())
	})

	t.Run("disable suggestion by setting threshold to zero", func(t *testing.T) {
		limit = 1 // lower total limit
		require.True(t, reg.SuggestContinueAsNew())

		suggestCAN = 0 // disable CAN suggestion
		require.False(t, reg.SuggestContinueAsNew())
	})

	t.Run("disable suggestion by setting total limit to zero", func(t *testing.T) {
		suggestCAN = 1 // enable CAN suggestion
		require.True(t, reg.SuggestContinueAsNew())

		limit = 0 // disable total limit
		require.False(t, reg.SuggestContinueAsNew())
	})
}

func assertRejectUpdateInRegistry(
	t *testing.T,
	reg update.Registry,
	evStore mockEventStore,
	upd *update.Update,
) {
	t.Helper()
	startRegistryLen := reg.Len()
	require.NoError(t, reject(t, evStore, upd), "update should be rejected")
	assertCompleted(t, upd, rejectionOutcome)
	require.Equal(t, startRegistryLen-1, reg.Len(), "update should have been removed")
}

func assertCompleteUpdateInRegistry(
	t *testing.T,
	reg update.Registry,
	evStore mockEventStore,
	upd *update.Update,
) {
	t.Helper()
	startRegistryLen := reg.Len()
	require.NoError(t, respondSuccess(t, evStore, upd), "update should be completed")
	assertCompleted(t, upd, successOutcome)
	require.Equal(t, startRegistryLen-1, reg.Len(), "update should have been removed")
}

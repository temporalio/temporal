package scheduler_test

import (
	"context"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// patchRequest returns a patch request that creates two backfillers: one for
// the immediate trigger, one for the range backfill.
func patchRequest(requestID string) *schedulerpb.PatchScheduleRequest {
	base := time.Now().UTC().Add(-time.Hour)
	return &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			RequestId:  requestID,
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
				BackfillRequest: []*schedulepb.BackfillRequest{
					{
						StartTime: timestamppb.New(base),
						EndTime:   timestamppb.New(base.Add(30 * time.Minute)),
					},
				},
			},
		},
	}
}

// backfillerIDs returns the scheduler's current backfill IDs. Backfillers are
// removed as they complete, so tests compare ID sets across a patch rather than
// counting survivors.
func backfillerIDs(sched *scheduler.Scheduler) []string {
	ids := make([]string, 0, len(sched.Backfillers))
	for id := range sched.Backfillers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func addedBackfillerIDs(before, after []string) []string {
	var added []string
	for _, id := range after {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}
	return added
}

func serializedConflictToken(t int64) []byte {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, uint64(t))
	return token
}

// TestPatch_DuplicateRequestIDDoesNotDuplicateActions pins that a retried
// PatchSchedule carrying the same request ID is applied exactly once. Each
// TriggerImmediately and BackfillRequest adds its own Backfiller, which goes on
// to start workflows, so a re-applied patch runs the manual actions twice.
func TestPatch_DuplicateRequestIDDoesNotDuplicateActions(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	_, err := sched.Patch(ctx, patchRequest("patch-request-1"))
	require.NoError(t, err)
	firstPatchIDs := backfillerIDs(sched)
	require.Len(t, firstPatchIDs, 2)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	// Retry of the same request, e.g. after the caller timed out.
	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Patch(ctx, patchRequest("patch-request-1"))
	require.NoError(t, err, "a retried patch must succeed idempotently")
	require.Empty(t, addedBackfillerIDs(firstPatchIDs, backfillerIDs(sched)),
		"retried patch with the same request ID must not create additional backfillers")
}

// TestPatch_DistinctRequestIDsApplyIndependently guards against over-deduping:
// two genuinely different patches must both be applied.
func TestPatch_DistinctRequestIDsApplyIndependently(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	_, err := sched.Patch(ctx, patchRequest("patch-request-1"))
	require.NoError(t, err)
	firstPatchIDs := backfillerIDs(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Patch(ctx, patchRequest("patch-request-2"))
	require.NoError(t, err)
	require.Len(t, addedBackfillerIDs(firstPatchIDs, backfillerIDs(sched)), 2)
}

// TestPatch_EmptyRequestIDIsNotDeduped documents that dedup is opt-in on the
// request ID: PatchSchedule does not require one, and patches without an ID
// keep their pre-existing at-least-once behavior.
func TestPatch_EmptyRequestIDIsNotDeduped(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	_, err := sched.Patch(ctx, patchRequest(""))
	require.NoError(t, err)
	firstPatchIDs := backfillerIDs(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Patch(ctx, patchRequest(""))
	require.NoError(t, err)
	require.Len(t, addedBackfillerIDs(firstPatchIDs, backfillerIDs(sched)), 2)
}

// TestUpdate_DuplicateRequestIDIsIdempotent pins that a retried UpdateSchedule
// carrying the same request ID succeeds instead of failing on its (already
// consumed) conflict token, and does not bump the conflict token twice.
func TestUpdate_DuplicateRequestIDIsIdempotent(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	initialToken := sched.ConflictToken
	updateRequest := func() *schedulerpb.UpdateScheduleRequest {
		return &schedulerpb.UpdateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:     namespace,
				ScheduleId:    scheduleID,
				Schedule:      defaultSchedule(),
				RequestId:     "update-request-1",
				ConflictToken: serializedConflictToken(initialToken),
			},
		}
	}

	_, err := sched.Update(ctx, updateRequest())
	require.NoError(t, err)
	require.Equal(t, initialToken+1, sched.ConflictToken)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, updateRequest())
	require.NoError(t, err, "a retried update must succeed idempotently")
	require.Equal(t, initialToken+1, sched.ConflictToken,
		"retried update with the same request ID must not bump the conflict token again")
}

// TestUpdate_DistinctRequestIDStillConflicts guards against over-deduping: a
// genuinely different update carrying a stale conflict token must still be
// rejected.
func TestUpdate_DistinctRequestIDStillConflicts(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	initialToken := sched.ConflictToken
	_, err := sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:     namespace,
			ScheduleId:    scheduleID,
			Schedule:      defaultSchedule(),
			RequestId:     "update-request-1",
			ConflictToken: serializedConflictToken(initialToken),
		},
	})
	require.NoError(t, err)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:     namespace,
			ScheduleId:    scheduleID,
			Schedule:      defaultSchedule(),
			RequestId:     "update-request-2",
			ConflictToken: serializedConflictToken(initialToken),
		},
	})
	require.ErrorIs(t, err, scheduler.ErrConflictTokenMismatch)
}

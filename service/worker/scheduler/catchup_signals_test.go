package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/testsuite"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// unhandledSignalsLog is what the SDK logs when a run completes or continues-as-new with
// signals it never received; those signals are lost.
const unhandledSignalsLog = "Workflow has unhandled signals"

// sendSignals queues signals for the workflow under test, delivered as it starts.
func sendSignals(signals ...func(env *testsuite.TestWorkflowEnvironment)) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			for _, s := range signals {
				s(env)
			}
		}, 0)
	}
}

func patchSignal(p *schedulepb.SchedulePatch) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) { env.SignalWorkflow(SignalNamePatch, p) }
}

// TestChunkedCatchupSignals checks that signals that arrive while a schedule is catching up in
// chunks are applied. Signals are otherwise only received while the scheduler sleeps, and a
// chunk's run continues-as-new without sleeping: before the fix, the SDK dropped them at
// continue-as-new ("Workflow has unhandled signals"), so a schedule couldn't be paused,
// updated or triggered until the catch-up finished.
func TestChunkedCatchupSignals(t *testing.T) {
	const chunk = 1000
	// 3h behind with a 1h window: the first chunk is all catchup-window misses.
	catchingUp := func() *schedulespb.StartScheduleArgs {
		args := newCatchupArgs(t, 3*time.Hour)
		args.Schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
		return args
	}
	firstChunk := func(t *testing.T, signals ...func(env *testsuite.TestWorkflowEnvironment)) catchupResult {
		r := runCatchupRepro(t, catchupCase{chunked: chunk, args: catchingUp(), beforeRun: sendSignals(signals...)})
		require.NoError(t, r.err)
		require.Equal(t, chunk, r.missedWindow, "the run is a chunk of misses")
		require.True(t, r.next.State.LastProcessedTime.AsTime().Before(baseStartTime), "the catch-up continues")
		require.Zero(t, r.logger.count(unhandledSignalsLog), "a signal was dropped")
		return r
	}

	t.Run("pause", func(t *testing.T) {
		r := firstChunk(t, patchSignal(&schedulepb.SchedulePatch{Pause: "paused during catch-up"}))
		require.True(t, r.next.Schedule.State.Paused)
		require.Equal(t, "paused during catch-up", r.next.Schedule.State.Notes)
	})

	t.Run("trigger-and-pause", func(t *testing.T) {
		// Two patches: each is applied, not just the last one received.
		r := firstChunk(t,
			patchSignal(&schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}),
			patchSignal(&schedulepb.SchedulePatch{Pause: "p"}),
		)
		require.True(t, r.next.Schedule.State.Paused)
		// Nothing starts until the catch-up finishes: the trigger waits in the buffer.
		require.Empty(t, r.startedIDs)
		require.Equal(t, []time.Time{baseStartTime}, bufferedNominalTimes(r.next))

		// Paused, the next run skips the rest of the range, finishing the catch-up, and
		// starts the trigger (manual starts run while paused). It's unpaused a minute later
		// only so that it wakes up and continues-as-new: paused, it would sleep until the
		// harness's long-poll stub times out.
		r2 := runCatchupRepro(t, catchupCase{chunked: chunk, args: r.next, beforeRun: func(env *testsuite.TestWorkflowEnvironment) {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(SignalNamePatch, &schedulepb.SchedulePatch{Unpause: "u"})
			}, time.Minute)
		}})
		require.NoError(t, r2.err)
		require.Equal(t, []string{"myid-" + baseStartTime.Format(time.RFC3339)}, r2.startedIDs)
		require.Empty(t, r2.next.State.BufferedStarts)
	})

	t.Run("update", func(t *testing.T) {
		args := catchingUp()
		updated := proto.Clone(args.Schedule).(*schedulepb.Schedule)
		updated.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
		r := firstChunk(t, func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(SignalNameUpdate, &schedulespb.FullUpdateRequest{
				Schedule:      updated,
				ConflictToken: args.State.ConflictToken,
			})
		})
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, r.next.Schedule.Policies.OverlapPolicy)
		require.Equal(t, args.State.ConflictToken+1, r.next.State.ConflictToken)
		require.Equal(t, baseStartTime, r.next.Info.UpdateTime.AsTime())

		// As after any update, matches before the update time are discarded, so the rest of
		// the catch-up starts only the match at the update time itself. The discarded ones
		// are still walked, a chunk per run.
		args = r.next
		var started []string
		for runs := 1; args.State.LastProcessedTime.AsTime().Before(baseStartTime); runs++ {
			require.Less(t, runs, 20, "catch-up did not converge")
			r := runCatchupRepro(t, catchupCase{chunked: chunk, args: args})
			require.NoError(t, r.err)
			started = append(started, r.startedIDs...)
			args = r.next
		}
		require.Equal(t, []string{"myid-" + baseStartTime.Format(time.RFC3339)}, started)
		require.Equal(t, r.next.Info.MissedCatchupWindow, args.Info.MissedCatchupWindow, "discarded matches are not misses")
	})

	t.Run("no-signals", func(t *testing.T) {
		r := firstChunk(t)
		require.False(t, r.next.Schedule.State.Paused)
		require.Equal(t, catchingUp().State.ConflictToken, r.next.State.ConflictToken)
	})
}

// TestChunkedCatchupLeavesBackfillsUnchanged checks that a backfill, which walks its range
// with manual set, is neither chunked nor cut short by the shortcuts at ChunkedCatchup: it
// does exactly what it did before. Under Skip, the skip_policy shortcut would otherwise end
// the backfill after its first start.
func TestChunkedCatchupLeavesBackfillsUnchanged(t *testing.T) {
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			backfill := func(chunked int) catchupResult {
				r := runCatchupRepro(t, catchupCase{
					chunked: chunked,
					args:    newCatchupArgs(t, 0), // not behind: only the backfill walks
					beforeRun: sendSignals(patchSignal(&schedulepb.SchedulePatch{
						BackfillRequest: []*schedulepb.BackfillRequest{{
							StartTime:     timestamppb.New(baseStartTime.Add(-2 * time.Hour)),
							EndTime:       timestamppb.New(baseStartTime.Add(-time.Hour)),
							OverlapPolicy: policy,
						}},
					})),
				})
				require.NoError(t, r.err)
				return r
			}
			want := backfill(0)
			got := backfill(50) // a chunk limit far below the backfill's 3600 matches
			require.NotEmpty(t, want.startedIDs)
			require.Equal(t, want.startedIDs, got.startedIDs)
			require.Equal(t, bufferedNominalTimes(want.next), bufferedNominalTimes(got.next))
			require.Len(t, got.next.State.OngoingBackfills, len(want.next.State.OngoingBackfills))
			for i := range want.next.State.OngoingBackfills {
				require.True(t, proto.Equal(want.next.State.OngoingBackfills[i], got.next.State.OngoingBackfills[i]))
			}
			require.Equal(t, want.next.Info.OverlapSkipped, got.next.Info.OverlapSkipped)
			require.Equal(t, want.next.Info.BufferDropped, got.next.Info.BufferDropped)
		})
	}
}

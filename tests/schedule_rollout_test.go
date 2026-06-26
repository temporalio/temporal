package tests

import (
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestScheduleRollout demonstrates that the percentage-based rollout control for
// CHASM (V2) scheduler creation can be ramped independently per namespace.
//
// Routing of a CreateSchedule call to V1 (workflow-based) vs V2 (CHASM) is decided
// in WorkflowHandler.chasmSchedulerCreationEnabled: the per-namespace
// EnableCHASMSchedulerCreation gate must be on, after which a stable hash of
// "<namespace>\x00<scheduleID>" (dynamicconfig.RolloutAccepts) is compared against
// the per-namespace CHASMSchedulerCreationRolloutPercent. Because the hash is keyed
// on the namespace name, each namespace ramps independently.
//
// The test exercises the percentage path (NOT the chasm-scheduler experiment header,
// which would force V2 unconditionally) across three namespaces:
//   - v2Full:    EnableCHASMSchedulerCreation=true, percent=100 -> every schedule V2
//   - v2Half:    EnableCHASMSchedulerCreation=true, percent=50  -> roughly half V2
//   - v1Default: no override (gate off)                         -> every schedule V1
func TestScheduleRollout(t *testing.T) {
	// EnableChasm turns on the CHASM framework cluster-wide; it does not by itself
	// force schedules onto V2 (that still requires EnableCHASMSchedulerCreation, which
	// we leave off for the default namespace). WithWorkerService runs the V1 scheduler
	// stack so V1 creates behave as in production; it also implies a dedicated cluster.
	s := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithWorkerService("V1 scheduler"),
	)

	// v1Default reuses the env's auto-created namespace (no rollout override).
	v1DefaultName := s.Namespace().String()
	v1DefaultID := s.NamespaceID().String()

	// Register two additional namespaces that opt into the CHASM rollout.
	v2FullName := testcore.RandomizeStr("sched-rollout-v2full")
	v2FullNsID, err := s.RegisterNamespace(namespace.Name(v2FullName), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)
	v2FullID := v2FullNsID.String()

	v2HalfName := testcore.RandomizeStr("sched-rollout-v2half")
	v2HalfNsID, err := s.RegisterNamespace(namespace.Name(v2HalfName), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)
	v2HalfID := v2HalfNsID.String()

	// Apply the rollout settings independently per namespace.
	cluster := s.GetTestCluster()
	cv := func(ns string, v any) dynamicconfig.ConstrainedValue {
		return dynamicconfig.ConstrainedValue{Constraints: dynamicconfig.Constraints{Namespace: ns}, Value: v}
	}
	// this is the original flag that was created for release rollout at the
	// namespace level. Because it already exists, it needs to be rolled out
	// AND the creationRolloutPercent also needs to be ramped up to for schedulers
	// to be created.
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableCHASMSchedulerCreation, []dynamicconfig.ConstrainedValue{
		cv(v2FullName, true),
		cv(v2HalfName, true),
		// v1DefaultName intentionally omitted -> gate stays false -> always V1.
	})

	// This is the second, newer, percentage based config rollout
	// note that in this instance the first NS is going to be always creating schedules on v2 and the
	// second only 50% (for the sake of the test)
	cluster.OverrideDynamicConfig(t, dynamicconfig.CHASMSchedulerCreationRolloutPercent, []dynamicconfig.ConstrainedValue{
		cv(v2FullName, 100),
		cv(v2HalfName, 50),
	})

	ctx := s.Context()

	// Use a plain context (no experiment header) so the percentage rollout, not the
	// header, decides routing.
	createSchedule := func(nsName, sid string) {
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   sid + "-wf",
						WorkflowType: &commonpb.WorkflowType{Name: "rollout-wf"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: "rollout-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			// Paused so nothing actually fires; we only care about creation-time routing.
			State: &schedulepb.ScheduleState{Paused: true},
		}
		_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "rollout-test",
			RequestId:  testcore.RandomizeStr("req"),
		})
		s.NoError(err)
	}

	// createdOnV2 reports whether the schedule lives on the CHASM (V2) stack. The CHASM
	// scheduler service returns the schedule for a real V2 entity, and NotFound for a
	// V1 schedule (there is no CHASM entity, only at most a sentinel). This mirrors the
	// discriminator used by the existing sentinel tests.
	createdOnV2 := func(nsName, nsID, sid string) bool {
		var v2 bool
		s.Eventually(func() bool {
			_, descErr := cluster.SchedulerClient().DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
			})
			if descErr == nil {
				v2 = true
				return true
			}
			var notFound *serviceerror.NotFound
			if errors.As(descErr, &notFound) {
				v2 = false
				return true
			}
			return false // unexpected error: retry
		}, 15*time.Second, 200*time.Millisecond, "CHASM DescribeSchedule should return OK or NotFound for %s/%s", nsName, sid)
		return v2
	}

	countV2 := func(nsName, nsID, prefix string, n int) int {
		v2 := 0
		for i := 0; i < n; i++ {
			sid := testcore.RandomizeStr(prefix)
			createSchedule(nsName, sid)
			if createdOnV2(nsName, nsID, sid) {
				v2++
			}
		}
		return v2
	}

	// The dynamic config overrides may take a moment to propagate to the frontend.
	// Wait until a freshly-created schedule in the 100% namespace routes to V2 before
	// running the deterministic assertions below.
	s.Eventually(func() bool {
		sid := testcore.RandomizeStr("rollout-probe")
		createSchedule(v2FullName, sid)
		return createdOnV2(v2FullName, v2FullID, sid)
	}, 30*time.Second, time.Second, "rollout config should propagate so 100%% namespace routes to V2")

	t.Run("FullRollout_AlwaysV2", func(t *testing.T) {
		const n = 20
		v2 := countV2(v2FullName, v2FullID, "v2full", n)
		s.Equal(n, v2, "every schedule in the 100%% namespace should be created on V2")
	})

	t.Run("DefaultNamespace_AlwaysV1", func(t *testing.T) {
		const n = 20
		v2 := countV2(v1DefaultName, v1DefaultID, "v1default", n)
		s.Equal(0, v2, "every schedule in the default (no-override) namespace should be created on V1")
	})

	t.Run("HalfRollout_RoughlyHalfV2", func(t *testing.T) {
		// The rollout is a stable hash of "<namespace>\x00<scheduleID>". The namespace
		// name is randomized per run, so the exact split varies; assert a wide band
		// (~5 sigma around the expected 50 of 100) plus a non-empty split in both
		// directions. This demonstrates ~50% routing without being flaky.
		const n = 100
		v2 := countV2(v2HalfName, v2HalfID, "v2half", n)
		t.Logf("50%% namespace: %d/%d schedules created on V2 (V1: %d)", v2, n, n-v2)
		s.Greater(v2, 0, "at least some schedules should be created on V2 at 50%%")
		s.Less(v2, n, "at least some schedules should be created on V1 at 50%%")
		s.GreaterOrEqual(v2, 25, "V2 count should be near 50%% (lower bound)")
		s.LessOrEqual(v2, 75, "V2 count should be near 50%% (upper bound)")
	})
}

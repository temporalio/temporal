package activityoptions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestMergeActivityOptionsAcceptance(t *testing.T) {
	updateOptions := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

	testCases := []struct {
		name      string
		mergeInto *activitypb.ActivityOptions
		mergeFrom *activitypb.ActivityOptions
		expected  *activitypb.ActivityOptions
		mask      *fieldmaskpb.FieldMask
	}{
		{
			name:      "Top-level fields with CamelCase",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"Priority",
					"RetryPolicy",
				},
			},
		},
		{
			name:      "Top-level fields with snake_case",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"start_to_close_timeout",
					"heartbeat_timeout",
					"priority",
					"retry_policy",
				},
			},
		},
		{
			name: "Sub-fields",
			mergeFrom: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mergeInto: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    10,
					FairnessKey:    "oldKey",
					FairnessWeight: 1.0,
				},
				RetryPolicy: &commonpb.RetryPolicy{},
			},
			expected: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"priority.priority_key",
					"priority.fairness_key",
					"priority.fairness_weight",
					"retry_policy.backoff_coefficient",
					"retry_policy.initial_interval",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updateFields := util.ParseFieldMask(tc.mask)
			err := MergeActivityOptions(tc.mergeInto, tc.mergeFrom, updateFields)
			require.NoError(t, err)
			require.Equal(t, tc.expected.RetryPolicy.GetInitialInterval(), tc.mergeInto.RetryPolicy.GetInitialInterval(), "RetryInitialInterval")
			require.Equal(t, tc.expected.RetryPolicy.GetMaximumInterval(), tc.mergeInto.RetryPolicy.GetMaximumInterval(), "RetryMaximumInterval")
			require.Equal(t, tc.expected.RetryPolicy.GetBackoffCoefficient(), tc.mergeInto.RetryPolicy.GetBackoffCoefficient(), "RetryBackoffCoefficient")
			require.Equal(t, tc.expected.RetryPolicy.GetMaximumAttempts(), tc.mergeInto.RetryPolicy.GetMaximumAttempts(), "RetryMaximumAttempts")
			require.Equal(t, tc.expected.TaskQueue, tc.mergeInto.TaskQueue, "TaskQueue")
			require.Equal(t, tc.expected.ScheduleToCloseTimeout, tc.mergeInto.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
			require.Equal(t, tc.expected.ScheduleToStartTimeout, tc.mergeInto.ScheduleToStartTimeout, "ScheduleToStartTimeout")
			require.Equal(t, tc.expected.StartToCloseTimeout, tc.mergeInto.StartToCloseTimeout, "StartToCloseTimeout")
			require.Equal(t, tc.expected.HeartbeatTimeout, tc.mergeInto.HeartbeatTimeout, "HeartbeatTimeout")
			require.Equal(t, tc.expected.Priority, tc.mergeInto.Priority, "Priority")
		})
	}
}

func TestMergeActivityOptionsErrors(t *testing.T) {
	makeReq := func(paths ...string) map[string]struct{} {
		return util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: paths})
	}
	emptyOpts := &activitypb.ActivityOptions{}

	var err error
	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.maximum_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.maximum_attempts"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.backoff_coefficient"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("retry_policy.initial_interval"))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("taskQueue.name"))
	require.ErrorContains(t, err, "TaskQueue is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.priority_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.fairness_key"))
	require.ErrorContains(t, err, "Priority is not provided")

	err = MergeActivityOptions(&activitypb.ActivityOptions{}, emptyOpts, makeReq("priority.fairness_weight"))
	require.ErrorContains(t, err, "Priority is not provided")
}

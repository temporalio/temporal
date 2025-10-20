package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/retrypolicy"
	"google.golang.org/protobuf/types/known/durationpb"
)

type testCase struct {
	name      string
	validator RequestAttributesValidator
}

const (
	defaultActivityID       = "test-activity-id"
	defaultActivityType     = "test-activity-type"
	defaultTaskQueue        = "test-task-queue"
	defaultMaxIDLengthLimit = 1000
	defaultNamespaceID      = "default"
)

var (
	defaultActvityOptions = activitypb.ActivityOptions{
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
	}
	defaultPriority = commonpb.Priority{FairnessKey: "normal"}
)

func TestValidateSuccess(t *testing.T) {
	validator := NewRequestAttributesValidator(
		defaultActivityID,
		defaultActivityType,
		defaultRetrySettings,
		defaultMaxIDLengthLimit,
		defaultNamespaceID,
		&defaultActvityOptions,
		&defaultPriority,
	)

	err := validator.ValidateAndAdjustTimeouts(durationpb.New(0))
	require.NoError(t, err)
}

func TestValidateFailures(t *testing.T) {
	cases := []testCase{
		{
			name: "Empty ActivityId",
			validator: NewRequestAttributesValidator(
				"",
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&defaultActvityOptions,
				&defaultPriority,
			),
		},
		{
			name: "Empty ActivityType",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				"",
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&defaultActvityOptions,
				&defaultPriority,
			),
		},
		{
			name: "ActivityId exceeds length limit",
			validator: NewRequestAttributesValidator(
				string(make([]byte, 1001)),
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&defaultActvityOptions,
				&defaultPriority,
			),
		},
		{
			name: "ActivityType exceeds length limit",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				string(make([]byte, 1001)),
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&defaultActvityOptions,
				&defaultPriority,
			),
		},
		{
			name: "Invalid TaskQueue",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&activitypb.ActivityOptions{
					TaskQueue:              &taskqueuepb.TaskQueue{Name: ""},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				},
				&defaultPriority,
			),
		},
		{
			name: "Negative ScheduleToCloseTimeout",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&activitypb.ActivityOptions{
					TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
					ScheduleToCloseTimeout: durationpb.New(-1 * time.Second),
				},
				&defaultPriority,
			),
		},
		{
			name: "Negative ScheduleToStartTimeout",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&activitypb.ActivityOptions{
					TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					ScheduleToStartTimeout: durationpb.New(-1 * time.Second),
				},
				&defaultPriority,
			),
		},
		{
			name: "Negative StartToCloseTimeout",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&activitypb.ActivityOptions{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
					StartToCloseTimeout: durationpb.New(-1 * time.Second),
				},
				&defaultPriority,
			),
		},
		{
			name: "Negative HeartbeatTimeout",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&activitypb.ActivityOptions{
					TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					HeartbeatTimeout:       durationpb.New(-1 * time.Second),
				},
				&defaultPriority,
			),
		},
		{
			name: "Invalid Priority",
			validator: NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				&defaultActvityOptions,
				&commonpb.Priority{FairnessKey: string(make([]byte, 1001))},
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.validator.ValidateAndAdjustTimeouts(durationpb.New(0))
			require.Error(t, err)
		})
	}
}

func TestAdjustActivityTimeouts(t *testing.T) {
	cases := []struct {
		name       string
		options    *activitypb.ActivityOptions
		runTimeout *durationpb.Duration
		isErr      bool
		validate   func(t *testing.T, opts *activitypb.ActivityOptions)
	}{
		{
			name: "ScheduleToClose set - fills in missing timeouts",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 0*time.Second, opts.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "StartToClose set but not ScheduleToClose - fills from runTimeout",
			options: &activitypb.ActivityOptions{
				TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				StartToCloseTimeout: durationpb.New(5 * time.Second),
			},
			runTimeout: durationpb.New(20 * time.Second),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 20*time.Second, opts.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 5*time.Second, opts.GetStartToCloseTimeout().AsDuration())
			},
		},
		{
			name: "Neither ScheduleToClose nor StartToClose set - returns error",
			options: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
			},
			runTimeout: durationpb.New(0),
			isErr:      true,
			validate:   func(t *testing.T, opts *activitypb.ActivityOptions) {},
		},
		{
			name: "ScheduleToClose and StartToClose set - StartToClose capped by ScheduleToClose",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(15 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetStartToCloseTimeout().AsDuration())
			},
		},
		{
			name: "ScheduleToStart capped by ScheduleToClose",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				ScheduleToStartTimeout: durationpb.New(20 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetStartToCloseTimeout().AsDuration())
			},
		},
		{
			name: "HeartbeatTimeout capped by StartToClose",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(20 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				HeartbeatTimeout:       durationpb.New(15 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "All timeouts capped by runTimeout",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToStartTimeout: durationpb.New(25 * time.Second),
				StartToCloseTimeout:    durationpb.New(20 * time.Second),
				HeartbeatTimeout:       durationpb.New(15 * time.Second),
			},
			runTimeout: durationpb.New(10 * time.Second),
			isErr:      false,
			validate: func(t *testing.T, opts *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, opts.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, opts.GetHeartbeatTimeout().AsDuration())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewRequestAttributesValidator(
				defaultActivityID,
				defaultActivityType,
				defaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				tc.options,
				&defaultPriority,
			)

			err := validator.ValidateAndAdjustTimeouts(tc.runTimeout)
			if tc.isErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			tc.validate(t, validator.options)
		})
	}
}

func defaultRetrySettings(_ string) retrypolicy.DefaultRetrySettings {
	return retrypolicy.DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100.0,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            0,
	}
}

package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/retrypolicy"
	"google.golang.org/protobuf/types/known/durationpb"
)

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

	defaultBlobSizeLimitError = func(ns string) int {
		return 64
	}
	defaultBlobSizeLimitWarn = func(ns string) int {
		return 32
	}
)

func TestValidateSuccess(t *testing.T) {
	modifiedAttributes, err := ValidateActivityRequestAttributes(
		defaultActivityID,
		defaultActivityType,
		getDefaultRetrySettings,
		defaultMaxIDLengthLimit,
		defaultNamespaceID,
		&defaultActvityOptions,
		&defaultPriority,
		durationpb.New(0))
	require.NoError(t, err)
	require.NotNil(t, modifiedAttributes)
}

func TestValidateFailures(t *testing.T) {
	cases := []struct {
		name                            string
		activityID                      string
		activityType                    string
		getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
		maxIDLengthLimit                int
		namespaceID                     namespace.ID
		options                         *activitypb.ActivityOptions
		priority                        *commonpb.Priority
		runTimeout                      *durationpb.Duration
	}{
		{
			name:                            "Empty ActivityId",
			activityID:                      "",
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options:                         &defaultActvityOptions,
			priority:                        &defaultPriority,
			runTimeout:                      nil,
		},
		{
			name:                            "Empty ActivityType",
			activityID:                      defaultActivityID,
			activityType:                    "",
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options:                         &defaultActvityOptions,
			priority:                        &defaultPriority,
			runTimeout:                      nil,
		},
		{
			name:                            "ActivityId exceeds length limit",
			activityID:                      string(make([]byte, 1001)),
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options:                         &defaultActvityOptions,
			priority:                        &defaultPriority,
			runTimeout:                      nil,
		},
		{
			name:                            "ActivityType exceeds length limit",
			activityID:                      defaultActivityID,
			activityType:                    string(make([]byte, 1001)),
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options:                         &defaultActvityOptions,
			priority:                        &defaultPriority,
			runTimeout:                      nil,
		},
		{
			name:                            "Invalid TaskQueue",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: ""},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			priority:   &defaultPriority,
			runTimeout: nil,
		},
		{
			name:                            "Negative ScheduleToCloseTimeout",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(-1 * time.Second),
			},
			priority:   &defaultPriority,
			runTimeout: nil,
		},
		{
			name:                            "Negative ScheduleToStartTimeout",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				ScheduleToStartTimeout: durationpb.New(-1 * time.Second),
			},
			priority:   &defaultPriority,
			runTimeout: nil,
		},
		{
			name:                            "Negative StartToCloseTimeout",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				StartToCloseTimeout: durationpb.New(-1 * time.Second),
			},
			priority:   &defaultPriority,
			runTimeout: nil,
		},
		{
			name:                            "Negative HeartbeatTimeout",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				HeartbeatTimeout:       durationpb.New(-1 * time.Second),
			},
			priority:   &defaultPriority,
			runTimeout: nil,
		},
		{
			name:                            "Invalid Priority",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options:                         &defaultActvityOptions,
			priority:                        &commonpb.Priority{FairnessKey: string(make([]byte, 1001))},
			runTimeout:                      nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateActivityRequestAttributes(
				tc.activityID,
				tc.activityType,
				tc.getDefaultActivityRetrySettings,
				tc.maxIDLengthLimit,
				tc.namespaceID,
				tc.options,
				tc.priority,
				durationpb.New(0))
			require.Error(t, err)
		})
	}
}

func TestValidateStandAloneRequestIDTooLong(t *testing.T) {
	_, err := ValidateStandaloneActivity(
		defaultActivityID,
		defaultActivityType,
		defaultBlobSizeLimitError,
		defaultBlobSizeLimitWarn,
		32,
		log.NewNoopLogger(),
		defaultMaxIDLengthLimit,
		"default",
		string(make([]byte, 1001)),
		nil,
		nil,
		nil)
	require.Error(t, err)
}

func TestValidateStandAloneInputTooLarge(t *testing.T) {
	_, err := ValidateStandaloneActivity(
		defaultActivityID,
		defaultActivityType,
		defaultBlobSizeLimitError,
		defaultBlobSizeLimitWarn,
		65,
		log.NewNoopLogger(),
		defaultMaxIDLengthLimit,
		"default",
		"test-request-id",
		nil,
		nil,
		nil)
	require.Error(t, err)
}

func TestValidateStandAloneInputWarningSizeShouldSucceed(t *testing.T) {
	_, err := ValidateStandaloneActivity(
		defaultActivityID,
		defaultActivityType,
		defaultBlobSizeLimitError,
		defaultBlobSizeLimitWarn,
		48,
		log.NewNoopLogger(),
		defaultMaxIDLengthLimit,
		"default",
		"test-request-id",
		nil,
		nil,
		nil)
	require.NoError(t, err)
}

func TestModifiedActivityTimeouts(t *testing.T) {
	cases := []struct {
		name       string
		options    *activitypb.ActivityOptions
		runTimeout *durationpb.Duration
		isErr      bool
		validate   func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes)
	}{
		{
			name: "ScheduleToClose set - fills in missing timeouts",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 20*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 20*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 5*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
			},
		},
		{
			name: "Neither ScheduleToClose nor StartToClose set - returns error",
			options: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
			},
			runTimeout: durationpb.New(0),
			isErr:      true,
			validate:   func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {},
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
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 20*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 20*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, modifiedAttr *ModifiedActivityRequestAttributes) {
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.StartToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, modifiedAttr.HeartbeatTimeout.AsDuration())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modifiedAttributes, err := ValidateActivityRequestAttributes(
				defaultActivityID,
				defaultActivityType,
				getDefaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				tc.options,
				&defaultPriority,
				tc.runTimeout)

			if tc.isErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			tc.validate(t, modifiedAttributes)
		})
	}
}

func getDefaultRetrySettings(_ string) retrypolicy.DefaultRetrySettings {
	return retrypolicy.DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100.0,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            0,
	}
}

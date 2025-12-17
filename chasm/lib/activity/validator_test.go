package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
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
	defaultActivityOptions = activitypb.ActivityOptions{
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
	err := ValidateAndNormalizeActivityAttributes(
		defaultActivityID,
		defaultActivityType,
		getDefaultRetrySettings,
		defaultMaxIDLengthLimit,
		defaultNamespaceID,
		&defaultActivityOptions,
		&defaultPriority,
		durationpb.New(0))
	require.NoError(t, err)
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
			options:                         &defaultActivityOptions,
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
			options:                         &defaultActivityOptions,
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
			options:                         &defaultActivityOptions,
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
			options:                         &defaultActivityOptions,
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
			options:                         &defaultActivityOptions,
			priority:                        &commonpb.Priority{FairnessKey: string(make([]byte, 1001))},
			runTimeout:                      nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAndNormalizeActivityAttributes(
				tc.activityID,
				tc.activityType,
				tc.getDefaultActivityRetrySettings,
				tc.maxIDLengthLimit,
				tc.namespaceID,
				tc.options,
				tc.priority,
				durationpb.New(0))
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
		})
	}
}

func newTestFrontendHandler(
	blobSizeLimitError func(string) int,
	blobSizeLimitWarn func(string) int,
	maxIDLengthLimit int,
) *frontendHandler {
	return &frontendHandler{
		config: &Config{
			BlobSizeLimitError: blobSizeLimitError,
			BlobSizeLimitWarn:  blobSizeLimitWarn,
			MaxIDLengthLimit:   func() int { return maxIDLengthLimit },
		},
		logger: log.NewNoopLogger(),
	}
}

func TestValidateStandAloneRequestIDTooLong(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              string(make([]byte, 1001)),
		Input:                  payloads.EncodeString("test-input"),
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputTooLarge(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payloads.EncodeString(string(make([]byte, 1000))),
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputWarningSizeShouldSucceed(t *testing.T) {
	payload := payloads.EncodeString("test-input")
	payloadSize := payload.Size()

	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payload,
	}

	h := newTestFrontendHandler(
		func(ns string) int { return payloadSize + 1 },
		func(ns string) int { return payloadSize },
		defaultMaxIDLengthLimit,
	)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	require.NoError(t, err)
}

func TestValidateStandAlone_IDPolicyShouldDefault(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)

	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE, req.IdReusePolicy)
	require.Equal(t, enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL, req.IdConflictPolicy)
}

func TestModifiedActivityTimeouts(t *testing.T) {
	cases := []struct {
		name       string
		options    *activitypb.ActivityOptions
		runTimeout *durationpb.Duration
		isErr      bool
		validate   func(t *testing.T, options *activitypb.ActivityOptions)
	}{
		{
			name: "ScheduleToClose set - fills in missing timeouts",
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, options.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 20*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 5*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, options.HeartbeatTimeout.AsDuration())
			},
		},
		{
			name: "Neither ScheduleToClose nor StartToClose set - returns error",
			options: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
			},
			runTimeout: durationpb.New(0),
			isErr:      true,
			validate:   func(t *testing.T, options *activitypb.ActivityOptions) {},
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
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, options.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 0*time.Second, options.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 20*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.HeartbeatTimeout.AsDuration())
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
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.ScheduleToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.ScheduleToStartTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.StartToCloseTimeout.AsDuration())
				require.Equal(t, 10*time.Second, options.HeartbeatTimeout.AsDuration())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAndNormalizeActivityAttributes(
				defaultActivityID,
				defaultActivityType,
				getDefaultRetrySettings,
				defaultMaxIDLengthLimit,
				defaultNamespaceID,
				tc.options,
				&defaultPriority,
				tc.runTimeout)

			if tc.isErr {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				return
			}

			require.NoError(t, err)
			tc.validate(t, tc.options)
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

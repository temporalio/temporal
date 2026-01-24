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

// DO NOT SUBMIT: fix callers to work with a pointer (go/goprotoapi-findings#message-value)
var (
	defaultActivityOptions = activitypb.ActivityOptions_builder{
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval: durationpb.New(1 * time.Second),
		}.Build(),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
	}.Build()

	defaultPriority = commonpb.Priority_builder{FairnessKey: "normal"}.Build()

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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: ""}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			}.Build(),
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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(-1 * time.Second),
			}.Build(),
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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				ScheduleToStartTimeout: durationpb.New(-1 * time.Second),
			}.Build(),
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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:           taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				StartToCloseTimeout: durationpb.New(-1 * time.Second),
			}.Build(),
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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				HeartbeatTimeout:       durationpb.New(-1 * time.Second),
			}.Build(),
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
			priority:                        commonpb.Priority_builder{FairnessKey: string(make([]byte, 1001))}.Build(),
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
	req := workflowservice.StartActivityExecutionRequest_builder{
		ActivityId:   defaultActivityID,
		ActivityType: commonpb.ActivityType_builder{Name: defaultActivityType}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval: durationpb.New(1 * time.Second),
		}.Build(),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
		Namespace:              "default",
		RequestId:              string(make([]byte, 1001)),
		Input:                  payloads.EncodeString("test-input"),
	}.Build()

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputTooLarge(t *testing.T) {
	req := workflowservice.StartActivityExecutionRequest_builder{
		ActivityId:   defaultActivityID,
		ActivityType: commonpb.ActivityType_builder{Name: defaultActivityType}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval: durationpb.New(1 * time.Second),
		}.Build(),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payloads.EncodeString(string(make([]byte, 1000))),
	}.Build()

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputWarningSizeShouldSucceed(t *testing.T) {
	payload := payloads.EncodeString("test-input")
	payloadSize := payload.Size()

	req := workflowservice.StartActivityExecutionRequest_builder{
		ActivityId:   defaultActivityID,
		ActivityType: commonpb.ActivityType_builder{Name: defaultActivityType}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval: durationpb.New(1 * time.Second),
		}.Build(),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payload,
	}.Build()

	h := newTestFrontendHandler(
		func(ns string) int { return payloadSize + 1 },
		func(ns string) int { return payloadSize },
		defaultMaxIDLengthLimit,
	)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)
	require.NoError(t, err)
}

func TestValidateStandAlone_IDPolicyShouldDefault(t *testing.T) {
	req := workflowservice.StartActivityExecutionRequest_builder{
		ActivityId:   defaultActivityID,
		ActivityType: commonpb.ActivityType_builder{Name: defaultActivityType}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval: durationpb.New(1 * time.Second),
		}.Build(),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
		Namespace:              "default",
		RequestId:              "test-request-id",
	}.Build()

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := h.validateAndNormalizeStartActivityExecutionRequest(req)

	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE, req.GetIdReusePolicy())
	require.Equal(t, enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL, req.GetIdConflictPolicy())
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
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 0*time.Second, options.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "StartToClose set but not ScheduleToClose - fills from runTimeout",
			options: activitypb.ActivityOptions_builder{
				TaskQueue:           taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				StartToCloseTimeout: durationpb.New(5 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(20 * time.Second),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 20*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 5*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 0*time.Second, options.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "Neither ScheduleToClose nor StartToClose set - returns error",
			options: activitypb.ActivityOptions_builder{
				TaskQueue: taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
			}.Build(),
			runTimeout: durationpb.New(0),
			isErr:      true,
			validate:   func(t *testing.T, options *activitypb.ActivityOptions) {},
		},
		{
			name: "ScheduleToClose and StartToClose set - StartToClose capped by ScheduleToClose",
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(15 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 0*time.Second, options.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "ScheduleToStart capped by ScheduleToClose",
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				ScheduleToStartTimeout: durationpb.New(20 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 0*time.Second, options.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "HeartbeatTimeout capped by StartToClose",
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(20 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				HeartbeatTimeout:       durationpb.New(15 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(0),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 20*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 20*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetHeartbeatTimeout().AsDuration())
			},
		},
		{
			name: "All timeouts capped by runTimeout",
			options: activitypb.ActivityOptions_builder{
				TaskQueue:              taskqueuepb.TaskQueue_builder{Name: defaultTaskQueue}.Build(),
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToStartTimeout: durationpb.New(25 * time.Second),
				StartToCloseTimeout:    durationpb.New(20 * time.Second),
				HeartbeatTimeout:       durationpb.New(15 * time.Second),
			}.Build(),
			runTimeout: durationpb.New(10 * time.Second),
			isErr:      false,
			validate: func(t *testing.T, options *activitypb.ActivityOptions) {
				require.Equal(t, 10*time.Second, options.GetScheduleToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetScheduleToStartTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, 10*time.Second, options.GetHeartbeatTimeout().AsDuration())
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

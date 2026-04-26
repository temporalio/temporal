package activity

import (
	"fmt"
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
	"go.temporal.io/server/common/primitives"
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
	t.Run("StandaloneActivitySuccess", func(t *testing.T) {
		err := ValidateAndNormalizeStandaloneActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			&defaultActivityOptions,
			&defaultPriority,
			durationpb.New(0))
		require.NoError(t, err)
	})

	t.Run("EmbeddedActivitySuccess", func(t *testing.T) {
		err := ValidateAndNormalizeEmbeddedActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			&defaultActivityOptions,
			&defaultPriority,
			durationpb.New(0),
			defaultTaskQueue)
		require.NoError(t, err)
	})
}

func TestValidateAllActivityFailures(t *testing.T) {
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
		expectedErrMessage              string
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
			expectedErrMessage:              "activityId is not set",
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
			expectedErrMessage:              "activityType is not set",
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
			expectedErrMessage:              fmt.Sprintf("activityId exceeds length limit. Length=%d Limit=%d", 1001, defaultMaxIDLengthLimit),
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
			expectedErrMessage:              fmt.Sprintf("activityType exceeds length limit. Length=%d Limit=%d", 1001, defaultMaxIDLengthLimit),
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
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "invalid ScheduleToCloseTimeout",
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
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "invalid ScheduleToStartTimeout",
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
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "invalid StartToCloseTimeout",
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
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "invalid HeartbeatTimeout",
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
			expectedErrMessage:              "invalid priorities",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAndNormalizeActivityAttributes(
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
			if tc.expectedErrMessage != "" {
				require.Contains(t, invalidArgErr.Error(), tc.expectedErrMessage)
			}
		})
	}
}

func TestStandaloneActivityTaskQueueValidations(t *testing.T) {
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
		expectedErrMessage              string
	}{
		{
			name:                            "Disallow PerNSWorkerTaskQueue TaskQueue",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: fmt.Sprintf("cannot use internal per-namespace task queue:%s", primitives.PerNSWorkerTaskQueue),
		},
		{
			name:                            "Disallow Internal TaskQueue Prefix",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "/_sys/my-task-queue"},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "task queue name cannot start with reserved prefix /_sys/",
		},
		{
			name:                            "Disallow Empty TaskQueue",
			activityID:                      defaultActivityID,
			activityType:                    defaultActivityType,
			getDefaultActivityRetrySettings: getDefaultRetrySettings,
			maxIDLengthLimit:                defaultMaxIDLengthLimit,
			namespaceID:                     defaultNamespaceID,
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: ""},
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			},
			priority:           &defaultPriority,
			runTimeout:         nil,
			expectedErrMessage: "missing task queue name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAndNormalizeStandaloneActivity(
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
			require.Contains(t, invalidArgErr.Error(), tc.expectedErrMessage)
		})
	}
}

func TestEmbeddedActivityTaskQueueValidations(t *testing.T) {
	t.Run("Allow PerNSWorkerTaskQueue TaskQueue", func(t *testing.T) {
		options := &activitypb.ActivityOptions{
			TaskQueue:              &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		}

		err := ValidateAndNormalizeEmbeddedActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			options,
			&defaultPriority,
			durationpb.New(0),
			primitives.PerNSWorkerTaskQueue)
		require.NoError(t, err)
	})

	t.Run("Disallow PerNSWorkerTaskQueue TaskQueue", func(t *testing.T) {
		options := &activitypb.ActivityOptions{
			TaskQueue:              &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		}

		err := ValidateAndNormalizeEmbeddedActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			options,
			&defaultPriority,
			durationpb.New(0),
			defaultTaskQueue)

		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Error(), "cannot use internal per-namespace task queue")
	})

	t.Run("Disallow Internal TaskQueue Prefix", func(t *testing.T) {
		options := &activitypb.ActivityOptions{
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "/_sys/my-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		}

		err := ValidateAndNormalizeEmbeddedActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			options,
			&defaultPriority,
			durationpb.New(0),
			defaultTaskQueue)

		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Error(), "task queue name cannot start with reserved prefix /_sys/")
	})

	t.Run("Disallow Empty TaskQueue", func(t *testing.T) {
		options := &activitypb.ActivityOptions{
			TaskQueue:              &taskqueuepb.TaskQueue{Name: ""},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		}

		err := ValidateAndNormalizeEmbeddedActivity(
			defaultActivityID,
			defaultActivityType,
			getDefaultRetrySettings,
			defaultMaxIDLengthLimit,
			defaultNamespaceID,
			options,
			&defaultPriority,
			durationpb.New(0),
			defaultTaskQueue)

		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Error(), "missing task queue name")
	})
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
	err := validateAndNormalizeStartRequest(req, h.config.MaxIDLengthLimit(), h.config.BlobSizeLimitError, h.config.BlobSizeLimitWarn, h.logger, h.saMapperProvider, h.saValidator)
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
	err := validateAndNormalizeStartRequest(req, h.config.MaxIDLengthLimit(), h.config.BlobSizeLimitError, h.config.BlobSizeLimitWarn, h.logger, h.saMapperProvider, h.saValidator)
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
	err := validateAndNormalizeStartRequest(req, h.config.MaxIDLengthLimit(), h.config.BlobSizeLimitError, h.config.BlobSizeLimitWarn, h.logger, h.saMapperProvider, h.saValidator)
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
	err := validateAndNormalizeStartRequest(req, h.config.MaxIDLengthLimit(), h.config.BlobSizeLimitError, h.config.BlobSizeLimitWarn, h.logger, h.saMapperProvider, h.saValidator)

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
			err := validateAndNormalizeActivityAttributes(
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

func TestValidateDeleteActivityExecutionRequest(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeDeleteRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("SuccessWithRunID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
		}
		err := validateAndNormalizeDeleteRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("EmptyActivityID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: "",
		}
		err := validateAndNormalizeDeleteRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("ActivityIDTooLong", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
		}
		err := validateAndNormalizeDeleteRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("InvalidRunID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "not-a-valid-uuid",
		}
		err := validateAndNormalizeDeleteRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})
}

func TestValidateStartDelay(t *testing.T) {
	t.Run("NilDuration", func(t *testing.T) {
		err := validateStartDelay(nil)
		require.NoError(t, err)
	})

	t.Run("ZeroDuration", func(t *testing.T) {
		err := validateStartDelay(durationpb.New(0))
		require.NoError(t, err)
	})

	t.Run("ValidDuration", func(t *testing.T) {
		err := validateStartDelay(durationpb.New(5 * time.Second))
		require.NoError(t, err)
	})

	t.Run("NegativeDuration", func(t *testing.T) {
		err := validateStartDelay(durationpb.New(-1 * time.Second))
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Message, "invalid StartDelay")
	})
}

func getDefaultRetrySettings(_ string) retrypolicy.DefaultRetrySettings {
	return retrypolicy.DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100.0,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            0,
	}
}

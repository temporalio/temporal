package tests

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/operatorservice/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultStartToCloseTimeout = 1 * time.Minute
	defaultIdentity            = "test-worker"
)

var (
	defaultInput            = payloads.EncodeString("Input")
	defaultHeartbeatDetails = payloads.EncodeString("Heartbeat Details")
	defaultResult           = payloads.EncodeString("Done")
	defaultRetryPolicy      = &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    0,
		MaximumInterval:    durationpb.New(100 * time.Second),
	}
	defaultFailure = &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}
	defaultHeader = &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"key-1": payload.EncodeString("value-1"),
			"key-2": payload.EncodeString("value-2"),
		},
	}
	defaultSearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": sadefs.MustEncodeValue("value1", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		},
	}
	defaultUserMetadata = &sdkpb.UserMetadata{
		Summary: payload.EncodeString("test-summary"),
		Details: payload.EncodeString("test-details"),
	}
	defaultMaxIDLengthLimit = dynamicconfig.MaxIDLengthLimit.Get(
		dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger()))()
	// Safety margin for assertions involving timer tasks. Must exceed TimerProcessorMaxTimeShift to account for timer
	// queue processing jitter.
	timerSafetyMargin = dynamicconfig.TimerProcessorMaxTimeShift.Get(
		dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger()))() * 3 / 2
)

type standaloneActivityTestSuite struct {
	parallelsuite.Suite[*standaloneActivityTestSuite]
}

func TestStandaloneActivityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &standaloneActivityTestSuite{})
}

type standaloneActivityEnv struct {
	*testcore.TestEnv
}

func (s *standaloneActivityTestSuite) newTestEnv(opts ...testcore.TestOption) *standaloneActivityEnv {
	env := &standaloneActivityEnv{
		TestEnv: testcore.NewEnv(s.T(), opts...),
	}
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
			{Constraints: dynamicconfig.Constraints{Namespace: env.ExternalNamespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), activity.EnableCallbacks, nsValues(true))
	return env
}

func (s *standaloneActivityTestSuite) TestIDReusePolicy() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("RejectDuplicate", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId
		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		env.validateCompletion(s.Context(), t, activityID, runID, "new-worker")

		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			IdReusePolicy:       enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE,
		})
		require.Error(t, err)
	})

	t.Run("AllowDuplicateFailedOnly", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Failure:   defaultFailure,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		env.validateFailure(s.Context(), t, activityID, runID, nil, "new-worker")

		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			IdReusePolicy:       enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		})
		require.NoError(t, err)
	})
}

func (s *standaloneActivityTestSuite) TestIDConflictPolicy() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	t.Run("Fail", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		firstStartResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)

		startWithFail := func(requestID string) (*workflowservice.StartActivityExecutionResponse, error) {
			return env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   activityID,
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
				},
				StartToCloseTimeout: durationpb.New(1 * time.Minute),
				RequestId:           requestID,
			})
		}

		t.Run("SecondStartFails", func(t *testing.T) {
			_, err := startWithFail("different-request-id")
			var alreadyStartedErr *serviceerror.ActivityExecutionAlreadyStarted
			require.ErrorAs(t, err, &alreadyStartedErr)
			require.Equal(t, env.Tv().RequestID(), alreadyStartedErr.StartRequestId)
			require.Equal(t, firstStartResp.GetRunId(), alreadyStartedErr.RunId)
		})

		t.Run("SecondStartWithSameRequestIdReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithFail(env.Tv().RequestID())
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())
		})
	})

	t.Run("UseExisting", func(t *testing.T) {
		originalActivityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		firstStartResp := env.startAndValidateActivity(s.Context(), t, originalActivityID, taskQueue)

		startWithUseExisting := func(requestID string) (*workflowservice.StartActivityExecutionResponse, error) {
			return env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   originalActivityID,
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
				},
				StartToCloseTimeout: durationpb.New(1 * time.Minute),
				IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
				RequestId:           requestID,
			})
		}

		t.Run("SecondStartReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithUseExisting("different-request-id")
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())

			// Link should point to the existing activity run.
			link := resp.GetLink().GetActivity()
			require.NotNil(t, link)
			require.Equal(t, env.Namespace().String(), link.Namespace)
			require.Equal(t, originalActivityID, link.ActivityId)
			require.Equal(t, firstStartResp.RunId, link.RunId)
		})
		t.Run("SecondStartWithSameRequestIdReturnsExistingRun", func(t *testing.T) {
			resp, err := startWithUseExisting(env.Tv().RequestID())
			require.NoError(t, err)
			require.Equal(t, firstStartResp.RunId, resp.RunId)
			require.False(t, resp.GetStarted())
		})

		t.Run("OnConflictOptions", func(t *testing.T) {
			env.OverrideDynamicConfig(
				callback.AllowedAddresses,
				[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
			)

			onConflictOpts := &commonpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			}

			t.Run("AttachesToNewActivity", func(t *testing.T) {
				newActivityID := testcore.RandomizeStr(t.Name())
				newTaskQueue := testcore.RandomizeStr(t.Name())

				resp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
					Namespace:    env.Namespace().String(),
					ActivityId:   newActivityID,
					ActivityType: env.Tv().ActivityType(),
					Identity:     env.Tv().WorkerIdentity(),
					Input:        defaultInput,
					TaskQueue: &taskqueuepb.TaskQueue{
						Name: newTaskQueue,
					},
					StartToCloseTimeout: durationpb.New(1 * time.Minute),
					IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
					RequestId:           env.Tv().Any().String(),
					CompletionCallbacks: []*commonpb.Callback{
						{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/new-activity-cb"}}},
					},
					OnConflictOptions: onConflictOpts,
				})
				require.NoError(t, err)
				require.True(t, resp.GetStarted())

				descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: newActivityID,
					RunId:      resp.RunId,
				})
				require.NoError(t, err)
				require.Len(t, descResp.Callbacks, 1)
				require.Equal(t, "http://localhost/new-activity-cb", descResp.Callbacks[0].GetInfo().GetCallback().GetNexus().GetUrl())
			})

			t.Run("AttachesToExistingActivity", func(t *testing.T) {
				resp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
					Namespace:    env.Namespace().String(),
					ActivityId:   originalActivityID,
					ActivityType: env.Tv().ActivityType(),
					Identity:     env.Tv().WorkerIdentity(),
					Input:        defaultInput,
					TaskQueue: &taskqueuepb.TaskQueue{
						Name: taskQueue,
					},
					StartToCloseTimeout: durationpb.New(1 * time.Minute),
					IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
					RequestId:           env.Tv().Any().String(),
					CompletionCallbacks: []*commonpb.Callback{
						{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/existing-activity-cb"}}},
					},
					OnConflictOptions: onConflictOpts,
				})
				require.NoError(t, err)
				require.False(t, resp.GetStarted())
				require.Equal(t, firstStartResp.RunId, resp.RunId)

				descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: originalActivityID,
					RunId:      firstStartResp.RunId,
				})
				require.NoError(t, err)
				require.Len(t, descResp.Callbacks, 1)
				require.Equal(t, "http://localhost/existing-activity-cb", descResp.Callbacks[0].GetInfo().GetCallback().GetNexus().GetUrl())
			})

			t.Run("AttachesCallbacksAndLinksTogether", func(t *testing.T) {
				bothActivityID := testcore.RandomizeStr(t.Name())
				bothTaskQueue := testcore.RandomizeStr(t.Name())
				bothStartResp := env.startAndValidateActivity(ctx, t, bothActivityID, bothTaskQueue)

				attachedLinks := []*commonpb.Link{
					{
						Variant: &commonpb.Link_WorkflowEvent_{
							WorkflowEvent: &commonpb.Link_WorkflowEvent{
								Namespace:  env.Namespace().String(),
								WorkflowId: "both-wf",
								RunId:      "both-run",
								Reference: &commonpb.Link_WorkflowEvent_EventRef{
									EventRef: &commonpb.Link_WorkflowEvent_EventReference{
										EventId:   1,
										EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
									},
								},
							},
						},
					},
				}

				resp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
					Namespace:    env.Namespace().String(),
					ActivityId:   bothActivityID,
					ActivityType: env.Tv().ActivityType(),
					Identity:     env.Tv().WorkerIdentity(),
					Input:        defaultInput,
					TaskQueue: &taskqueuepb.TaskQueue{
						Name: bothTaskQueue,
					},
					StartToCloseTimeout: durationpb.New(1 * time.Minute),
					IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
					RequestId:           env.Tv().Any().String(),
					CompletionCallbacks: []*commonpb.Callback{
						{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/both-cb"}}},
					},
					Links:             attachedLinks,
					OnConflictOptions: onConflictOpts,
				})
				require.NoError(t, err)
				require.False(t, resp.GetStarted())
				require.Equal(t, bothStartResp.RunId, resp.RunId)

				descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: bothActivityID,
					RunId:      bothStartResp.RunId,
				})
				require.NoError(t, err)
				require.Len(t, descResp.Callbacks, 1)
				require.Equal(t, "http://localhost/both-cb", descResp.Callbacks[0].GetInfo().GetCallback().GetNexus().GetUrl())
				protorequire.ProtoSliceEqual(t, attachedLinks, descResp.GetInfo().GetLinks())
			})

			t.Run("IdempotentWithSameRequestId", func(t *testing.T) {
				idempotentActivityID := testcore.RandomizeStr(t.Name())
				idempotentTaskQueue := testcore.RandomizeStr(t.Name())
				idempotentStartResp := env.startAndValidateActivity(s.Context(), t, idempotentActivityID, idempotentTaskQueue)

				requestID := env.Tv().Any().String()
				startReq := &workflowservice.StartActivityExecutionRequest{
					Namespace:    env.Namespace().String(),
					ActivityId:   idempotentActivityID,
					ActivityType: env.Tv().ActivityType(),
					Identity:     env.Tv().WorkerIdentity(),
					Input:        defaultInput,
					TaskQueue: &taskqueuepb.TaskQueue{
						Name: idempotentTaskQueue,
					},
					StartToCloseTimeout: durationpb.New(1 * time.Minute),
					IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
					RequestId:           requestID,
					CompletionCallbacks: []*commonpb.Callback{
						{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/idempotent-cb"}}},
					},
					OnConflictOptions: onConflictOpts,
				}

				// First call attaches the callback.
				resp1, err := env.FrontendClient().StartActivityExecution(s.Context(), startReq)
				require.NoError(t, err)
				require.False(t, resp1.GetStarted())

				// Second call with the same request ID should not duplicate the callback.
				resp2, err := env.FrontendClient().StartActivityExecution(s.Context(), startReq)
				require.NoError(t, err)
				require.False(t, resp2.GetStarted())

				descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: idempotentActivityID,
					RunId:      idempotentStartResp.RunId,
				})
				require.NoError(t, err)
				// Only 1 callback: the second call with the same request ID should not add another.
				require.Len(t, descResp.Callbacks, 1)
				require.Equal(t, "http://localhost/idempotent-cb", descResp.Callbacks[0].GetInfo().GetCallback().GetNexus().GetUrl())
			})
		})

		t.Run("DoesNotApplyToCompletedActivity", func(t *testing.T) {
			pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, originalActivityID, taskQueue, firstStartResp.RunId)
			_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: pollTaskResp.TaskToken,
				Result:    defaultResult,
				Identity:  defaultIdentity,
			})
			require.NoError(t, err)

			// USE_EXISTING only applies to running activities; completed activities
			// are governed by reuse policy (default ALLOW_DUPLICATE creates new)
			resp, err := startWithUseExisting("different-request-id")
			require.NoError(t, err)
			require.NotEqual(t, firstStartResp.RunId, resp.RunId)
			require.True(t, resp.GetStarted())
		})
	})
}

func (s *standaloneActivityTestSuite) TestPollActivityTaskQueue() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("FirstAttempt", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		namespace := env.Namespace().String()

		startToCloseTimeout := durationpb.New(1 * time.Minute)
		scheduleToCloseTimeout := durationpb.New(2 * time.Minute)
		heartbeatTimeout := durationpb.New(20 * time.Second)
		priority := &commonpb.Priority{
			FairnessKey: "test-key",
		}

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    namespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			HeartbeatTimeout:       heartbeatTimeout,
			RequestId:              env.Tv().RequestID(),
			Priority:               priority,
			Header:                 defaultHeader,
		})
		require.NoError(t, err)

		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, pollTaskResp.GetActivityId())
		require.Equal(t, namespace, pollTaskResp.GetWorkflowNamespace())
		protorequire.ProtoEqual(t, env.Tv().ActivityType(), pollTaskResp.GetActivityType())
		require.Equal(t, startResp.GetRunId(), pollTaskResp.GetActivityRunId())
		protorequire.ProtoEqual(t, defaultInput, pollTaskResp.GetInput())
		require.False(t, pollTaskResp.GetStartedTime().AsTime().IsZero())
		require.False(t, pollTaskResp.GetScheduledTime().AsTime().IsZero())
		require.EqualValues(t, 1, pollTaskResp.Attempt)
		protorequire.ProtoEqual(t, startToCloseTimeout, pollTaskResp.GetStartToCloseTimeout())
		protorequire.ProtoEqual(t, scheduleToCloseTimeout, pollTaskResp.GetScheduleToCloseTimeout())
		protorequire.ProtoEqual(t, heartbeatTimeout, pollTaskResp.GetHeartbeatTimeout())
		protorequire.ProtoEqual(t, priority, pollTaskResp.GetPriority())
		protorequire.ProtoEqual(t, defaultHeader, pollTaskResp.GetHeader())
		require.NotNil(t, pollTaskResp.TaskToken)
		protorequire.ProtoEqual(t, pollTaskResp.GetScheduledTime(), pollTaskResp.GetCurrentAttemptScheduledTime()) // Equal on first attempt
	})

	t.Run("RetriedAttempt", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		namespace := env.Namespace().String()

		startToCloseTimeout := durationpb.New(1 * time.Minute)
		scheduleToCloseTimeout := durationpb.New(2 * time.Minute)
		heartbeatTimeout := durationpb.New(20 * time.Second)
		priority := &commonpb.Priority{
			FairnessKey: "test-key",
		}

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    namespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			HeartbeatTimeout:       heartbeatTimeout,
			RequestId:              env.Tv().RequestID(),
			Priority:               priority,
			Header:                 defaultHeader,
		})
		require.NoError(t, err)

		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		nextRetryDelay := durationpb.New(1 * time.Second)
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: nextRetryDelay,
				}},
			},
		})
		require.NoError(t, err)

		describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)

		pollTaskResp, err = env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, pollTaskResp.GetActivityId())
		require.Equal(t, namespace, pollTaskResp.GetWorkflowNamespace())
		protorequire.ProtoEqual(t, env.Tv().ActivityType(), pollTaskResp.GetActivityType())
		require.Equal(t, startResp.GetRunId(), pollTaskResp.GetActivityRunId())
		protorequire.ProtoEqual(t, defaultInput, pollTaskResp.GetInput())
		require.False(t, pollTaskResp.GetStartedTime().AsTime().IsZero())
		require.False(t, pollTaskResp.GetScheduledTime().AsTime().IsZero())
		require.EqualValues(t, 2, pollTaskResp.Attempt)
		protorequire.ProtoEqual(t, startToCloseTimeout, pollTaskResp.GetStartToCloseTimeout())
		protorequire.ProtoEqual(t, scheduleToCloseTimeout, pollTaskResp.GetScheduleToCloseTimeout())
		protorequire.ProtoEqual(t, heartbeatTimeout, pollTaskResp.GetHeartbeatTimeout())
		protorequire.ProtoEqual(t, priority, pollTaskResp.GetPriority())
		protorequire.ProtoEqual(t, defaultHeader, pollTaskResp.GetHeader())
		require.NotNil(t, pollTaskResp.TaskToken)

		expectedAttemptScheduledTime := timestamppb.New(
			describeResp.GetInfo().GetLastAttemptCompleteTime().AsTime().Add(nextRetryDelay.AsDuration()))
		protorequire.ProtoEqual(t, expectedAttemptScheduledTime, pollTaskResp.GetCurrentAttemptScheduledTime())
	})
}

func (s *standaloneActivityTestSuite) TestStart() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("RequestIDTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   env.Tv().ActivityID(),
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: env.Tv().TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           string(make([]byte, defaultMaxIDLengthLimit+1)),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   env.Tv().ActivityID(),
				ActivityType: env.Tv().ActivityType(),
				Identity:     string(make([]byte, defaultMaxIDLengthLimit+1)),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: env.Tv().TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           env.Tv().RequestID(),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InputTooLarge", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := env.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			input := payloads.EncodeString(string(make([]byte, blobSizeLimitError+1)))

			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   env.Tv().ActivityID(),
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        input,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: env.Tv().TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           env.Tv().RequestID(),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, "input exceeds length limit")
		})

		t.Run("SearchAttributesInvalid", func(t *testing.T) {
			invalidSearchAttributes := &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"InvalidSearchAttributeKey": sadefs.MustEncodeValue("value", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
			}

			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:    env.Namespace().String(),
				ActivityId:   env.Tv().ActivityID(),
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: env.Tv().TaskQueue().GetName(),
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           env.Tv().RequestID(),
				SearchAttributes:    invalidSearchAttributes,
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
		})
	})

	t.Run("ResponseFields", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		resp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		require.True(t, resp.Started)
		require.NotEmpty(t, resp.RunId)

		// Verify link points to the started activity.
		link := resp.GetLink().GetActivity()
		require.NotNil(t, link)
		require.Equal(t, env.Namespace().String(), link.Namespace)
		require.Equal(t, activityID, link.ActivityId)
		require.Equal(t, resp.RunId, link.RunId)
	})

	t.Run("RequestLinksSurfacedOnDescribe", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		requestLinks := []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  env.Namespace().String(),
						WorkflowId: "linked-workflow-id",
						RunId:      "linked-run-id",
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventId:   1,
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
				},
			},
			{
				Variant: &commonpb.Link_Activity_{
					Activity: &commonpb.Link_Activity{
						Namespace:  env.Namespace().String(),
						ActivityId: "linked-activity-id",
						RunId:      "linked-activity-run-id",
					},
				},
			},
		}

		resp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			Links:               requestLinks,
		})
		require.NoError(t, err)
		require.True(t, resp.Started)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      resp.RunId,
		})
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t, requestLinks, descResp.GetInfo().GetLinks())
	})

	t.Run("AttachLinksOnConflictUnionsLinks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		firstLinks := []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  env.Namespace().String(),
						WorkflowId: "first-wf",
						RunId:      "first-run",
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventId:   1,
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
				},
			},
		}
		secondLinks := []*commonpb.Link{
			{
				Variant: &commonpb.Link_Activity_{
					Activity: &commonpb.Link_Activity{
						Namespace:  env.Namespace().String(),
						ActivityId: "second-act",
						RunId:      "second-run",
					},
				},
			},
		}

		firstResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               firstLinks,
		})
		require.NoError(t, err)
		require.True(t, firstResp.Started)

		secondResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               secondLinks,
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachLinks: true,
			},
		})
		require.NoError(t, err)
		require.False(t, secondResp.Started)
		require.Equal(t, firstResp.RunId, secondResp.RunId)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      firstResp.RunId,
		})
		require.NoError(t, err)
		expected := append([]*commonpb.Link{}, firstLinks...)
		expected = append(expected, secondLinks...)
		// Links across requests are stored in a map keyed by request ID, so their relative order is non-deterministic.
		protorequire.ProtoElementsMatch(t, expected, descResp.GetInfo().GetLinks())
	})

	t.Run("AttachLinksOnConflictStoresRawInput", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startLinks := []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  env.Namespace().String(),
						WorkflowId: "raw-wf",
						RunId:      "raw-run",
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventId:   1,
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
				},
			},
		}

		firstResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               startLinks,
		})
		require.NoError(t, err)
		require.True(t, firstResp.Started)

		// Second start re-sends the same link plus an extra duplicate within the same batch.
		// Activity start does not dedup intra-batch (matching the workflow start path), so the
		// duplicate is preserved on the activity.
		_, err = env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               append(startLinks, startLinks...),
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachLinks: true,
			},
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      firstResp.RunId,
		})
		require.NoError(t, err)
		// Activity holds the first call's [link] under reqID-1 and the second call's
		// raw [link, link] under reqID-2 — no intra-batch dedup is applied.
		expected := append([]*commonpb.Link{}, startLinks...)
		expected = append(expected, startLinks...)
		expected = append(expected, startLinks...)
		protorequire.ProtoSliceEqual(t, expected, descResp.GetInfo().GetLinks())
	})

	t.Run("PerExecutionCapEnforcedOnCreate", func(t *testing.T) {
		const maxLinks = 2
		cleanup := env.OverrideDynamicConfig(dynamicconfig.MaxLinksPerComponent, maxLinks)
		defer cleanup()

		links := make([]*commonpb.Link, maxLinks+1)
		for i := range links {
			links[i] = &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  env.Namespace().String(),
						WorkflowId: fmt.Sprintf("cap-wf-%d", i),
						RunId:      "cap-run",
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventId:   1,
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
				},
			}
		}

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          testcore.RandomizeStr(t.Name()),
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr(t.Name())},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			Links:               links,
		})
		require.Error(t, err)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	})

	t.Run("PerExecutionCapNotEnforcedWhenLinksWillBeDropped", func(t *testing.T) {
		// Reproduces the SAA Nexus-handler scenario: a benign retry of
		// StartActivityExecution against an already-running activity with
		// USE_EXISTING and no OnConflictOptions.attach_links must succeed and
		// silently drop the request's Links field. The per-component cap must
		// not be enforced on the dropped links — without this carve-out, a
		// retry that would have pushed the activity over the cap (if attach
		// had been requested) would falsely reject.
		const maxLinks = 1
		cleanup := env.OverrideDynamicConfig(dynamicconfig.MaxLinksPerComponent, maxLinks)
		defer cleanup()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		makeLink := func(prefix string, i int) *commonpb.Link {
			return &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  env.Namespace().String(),
						WorkflowId: fmt.Sprintf("%s-wf-%d", prefix, i),
						RunId:      fmt.Sprintf("%s-run", prefix),
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventId:   1,
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
				},
			}
		}

		// First call seeds the activity at the per-component cap so any
		// subsequent attach would exceed it.
		firstLinks := []*commonpb.Link{makeLink("first", 0)}
		firstResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               firstLinks,
		})
		require.NoError(t, err)
		require.True(t, firstResp.Started)

		links := []*commonpb.Link{makeLink("drop", 0)}
		secondResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			Links:               links,
			// attach_links intentionally omitted — the Links field should be silently dropped.
		})
		require.NoError(t, err, "over-cap Links must be ignored when attach_links is unset")
		require.False(t, secondResp.Started)
		require.Equal(t, firstResp.RunId, secondResp.RunId)
	})
}

func (s *standaloneActivityTestSuite) TestComplete() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("ByToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		env.validateCompletion(s.Context(), t, activityID, runID, "new-worker")
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      runID,
			ActivityId: activityID,
			Result:     defaultResult,
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.validateCompletion(s.Context(), t, activityID, runID, env.Tv().WorkerIdentity())
	})

	t.Run("ByIDWithoutRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      runID,
			ActivityId: activityID,
			Result:     defaultResult,
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.validateCompletion(s.Context(), t, activityID, runID, env.Tv().WorkerIdentity())
	})

	t.Run("StaleToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Complete with stale token (activity already completed)
		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to complete with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and get task token for attempt 1
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		// Fail the task with NextRetryDelay to control retry timing
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to get attempt 2 (ensures retry has happened)
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Try to complete with the old attempt 1 token - should fail with NotFound
		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Result:    defaultResult,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))

		// Complete with the attempt 2 token and should succeed
		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)
	})

	t.Run("ByIDAfterRetry", func(t *testing.T) {
		// Regression test: by-ID completion must succeed on attempt 2+.
		// Previously the synthesized token hardcoded attempt=1, which caused
		// validateActivityTaskToken to reject the request with NotFound.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                  defaultInput,
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt 1 retryably.
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to start attempt 2.
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Complete by ID — this must succeed on attempt 2.
		_, err = env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      attempt2Resp.ActivityRunId,
			ActivityId: activityID,
			Result:     defaultResult,
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to complete with existing token
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Result:    defaultResult,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestFail() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("ByToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Failure:   defaultFailure,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		env.validateFailure(s.Context(), t, activityID, runID, nil, "new-worker")
	})

	t.Run("WithHeartbeatDetails", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace:            env.Namespace().String(),
			TaskToken:            pollTaskResp.TaskToken,
			Failure:              defaultFailure,
			LastHeartbeatDetails: defaultHeartbeatDetails,
			Identity:             env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.validateFailure(s.Context(), t, activityID, runID, defaultHeartbeatDetails, env.Tv().WorkerIdentity())
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskFailedById(s.Context(), &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      runID,
			ActivityId: activityID,
			Failure:    defaultFailure,
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.validateFailure(s.Context(), t, activityID, runID, nil, env.Tv().WorkerIdentity())
	})

	t.Run("ByIDWithoutRunID", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskFailedById(s.Context(), &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Failure:    defaultFailure,
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.validateFailure(s.Context(), t, activityID, runID, nil, env.Tv().WorkerIdentity())
	})

	t.Run("StaleToken", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Fail with stale token (activity already completed)
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure:   defaultFailure,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to complete with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and get task token for attempt 1
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		// Fail the task with NextRetryDelay to control retry timing
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to get attempt 2 (ensures retry has happened)
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Try to fail with the old attempt 1 token - should fail with NotFound
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))

		// Fail with the attempt 2 token and should be no error
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.NoError(t, err)
	})

	t.Run("ByIDAfterRetry", func(t *testing.T) {
		// Regression test: by-ID failure must succeed on attempt 2+.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                  defaultInput,
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt 1 retryably.
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to start attempt 2.
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Fail by ID with a non-retryable failure — must succeed on attempt 2.
		_, err = env.FrontendClient().RespondActivityTaskFailedById(s.Context(), &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      attempt2Resp.ActivityRunId,
			ActivityId: activityID,
			Failure: &failurepb.Failure{
				Message: "terminal failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				}},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to fail with existing token
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Failure:   defaultFailure,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestRequestCancel() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("ByToken", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		identity := "client-that-requested-cancellation"

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   identity,
			RequestId:  env.Tv().RequestID(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

		details := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				payload.EncodeString("Canceled Details"),
			},
		}

		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   details,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)

		info := activityResp.GetInfo()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus(),
			"expected Canceled but is %s", info.GetStatus())
		require.Equal(t, "Test Cancellation", info.GetCanceledReason())
		require.Equal(t, int64(1), info.GetTotalHeartbeatCount(), "total heartbeat count")
		require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
		require.NotNil(t, info.GetCloseTime())
		protorequire.ProtoEqual(t, details, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetDetails())
		require.Equal(t, identity, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetIdentity())
	})

	testByIDCases := []struct {
		name         string
		includeRunID bool
	}{
		{
			name:         "with run ID",
			includeRunID: true,
		},
		{
			name:         "without run ID",
			includeRunID: false,
		},
	}
	for _, tc := range testByIDCases {
		s.Run(tc.name, func(s *standaloneActivityTestSuite) {
			t := s.T()

			activityID := testcore.RandomizeStr(tc.name)
			taskQueue := testcore.RandomizeStr(tc.name)
			identity := "client-that-requested-cancellation"

			startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
			runID := startResp.RunId

			env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   identity,
				RequestId:  env.Tv().RequestID(),
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)

			heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeatById(s.Context(), &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

			details := &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					payload.EncodeString("Canceled Details"),
				},
			}

			cancelReq := &workflowservice.RespondActivityTaskCanceledByIdRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				Details:    details,
				Identity:   "new-worker",
			}

			if tc.includeRunID {
				cancelReq.RunId = runID
			}

			_, err = env.FrontendClient().RespondActivityTaskCanceledById(s.Context(), cancelReq)
			require.NoError(t, err)

			activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:      env.Namespace().String(),
				ActivityId:     activityID,
				RunId:          runID,
				IncludeInput:   true,
				IncludeOutcome: true,
			})
			require.NoError(t, err)

			info := activityResp.GetInfo()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus(),
				"expected Canceled but is %s", info.GetStatus())
			require.Equal(t, "Test Cancellation", info.GetCanceledReason())
			require.Equal(t, int64(1), info.GetTotalHeartbeatCount(), "total heartbeat count")
			protorequire.ProtoEqual(t, details, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetDetails())
			require.Equal(t, identity, activityResp.GetOutcome().GetFailure().GetCanceledFailureInfo().GetIdentity())
		})
	}

	t.Run("ByIDAfterRetry", func(t *testing.T) {

		// Regression test: by-ID cancel must succeed on attempt 2+.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                  defaultInput,
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt 1 retryably.
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to start attempt 2.
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Request cancellation first.
		_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      attempt2Resp.ActivityRunId,
			Identity:   env.Tv().WorkerIdentity(),
			RequestId:  env.Tv().Any().String(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		// Cancel by ID — must succeed on attempt 2.
		_, err = env.FrontendClient().RespondActivityTaskCanceledById(s.Context(), &workflowservice.RespondActivityTaskCanceledByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      attempt2Resp.ActivityRunId,
			ActivityId: activityID,
			Details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					payload.EncodeString("Canceled Details"),
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
	})

	t.Run("FailsIfNeverRequested", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		details := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				payload.EncodeString("Canceled Details"),
			},
		}

		_, err := env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   details,
			Identity:  "new-worker",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("DuplicateRequestIDSucceeds", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		for range 2 {
			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   "client-that-requested-cancellation",
				RequestId:  "cancel-request-id",
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)
		}

		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeatById(s.Context(), &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

		activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)

		info := activityResp.GetInfo()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
			"expected Running but is %s", info.GetStatus())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, info.GetRunState(),
			"expected CancelRequested but is %s", info.GetRunState())
		require.Equal(t, "Test Cancellation", info.GetCanceledReason())
		require.Equal(t, int64(1), info.GetTotalHeartbeatCount(), "total heartbeat count")
	})

	t.Run("DifferentRequestIDFails", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		identity := "client-that-requested-cancellation"

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   identity,
			RequestId:  "cancel-request-id",
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   identity,
			RequestId:  "different-cancel-request-id",
			Reason:     "Test Cancellation",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	testAfterFinishedCases := []struct {
		name             string
		taskCompletionFn func(context.Context, *testing.T, []byte, string, string) error
		expectedStatus   enumspb.ActivityExecutionStatus
	}{
		{
			name: "finish with completion",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: taskToken,
					Result:    defaultResult,
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name: "finish with failure",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: taskToken,
					Failure:   defaultFailure,
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
		},
		{
			name: "finish with termination",
			taskCompletionFn: func(ctx context.Context, t *testing.T, taskToken []byte, activityID string, runID string) error {
				_, err := env.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Reason:     "Test Termination",
				})

				return err
			},
			expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
	}
	for _, tc := range testAfterFinishedCases {
		s.Run(tc.name, func(s *standaloneActivityTestSuite) {
			t := s.T()

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
			runID := startResp.RunId

			pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
				Identity:   "client-that-requested-cancellation",
				RequestId:  env.Tv().RequestID(),
				Reason:     "Test Cancellation",
			})
			require.NoError(t, err)

			err = tc.taskCompletionFn(s.Context(), t, pollTaskResp.GetTaskToken(), activityID, runID)
			require.NoError(t, err)

			activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(t, err)

			info := activityResp.GetInfo()
			require.Equal(t, tc.expectedStatus, info.GetStatus())
		})
	}

	t.Run("Retry", func(t *testing.T) {
		runTest := func(t *testing.T, requestCancellation bool) {

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:              env.Namespace().String(),
				ActivityId:             activityID,
				ActivityType:           env.Tv().ActivityType(),
				TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
				ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
					MaximumAttempts: 2,
				},
			})
			require.NoError(t, err)
			runID := startResp.RunId

			pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, pollResp.Attempt)

			if requestCancellation {
				_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Identity:   "test",
					RequestId:  env.Tv().RequestID(),
				})
				require.NoError(t, err)
			}

			retryableFailure := &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			}
			_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: pollResp.TaskToken,
				Failure:   retryableFailure,
			})
			require.NoError(t, err)

			activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(t, err)

			if requestCancellation {
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, activityResp.GetInfo().GetStatus())
			} else {
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, activityResp.GetInfo().GetStatus())
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, activityResp.GetInfo().GetRunState())
			}
		}
		t.Run("AllowedIfNotCancelRequested", func(t *testing.T) {
			runTest(t, false)
		})
		t.Run("PreventedIfCancelRequested", func(t *testing.T) {
			runTest(t, true)
		})
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyActivityID", func(t *testing.T) {

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				Namespace: env.Namespace().String(),
				Reason:    "Test Cancellation",
				Identity:  "client-that-requested-cancellation",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  env.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "client-that-requested-cancellation",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("RequestIDTooLong", func(t *testing.T) {

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RequestId:  string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  env.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "client-that-requested-cancellation",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  env.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Namespace:  env.Namespace().String(),
				Reason:     "Test Cancellation",
				Identity:   "client-that-requested-cancellation",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})

		t.Run("ReasonTooLong", func(t *testing.T) {

			blobSizeLimitError := 1000
			cleanup := env.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  env.Namespace().String(),
				Reason:     string(make([]byte, blobSizeLimitError+1)),
				Identity:   "client-that-requested-cancellation",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
		})
	})

	t.Run("ImmediatelyCancelled_WhenInScheduledState", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "client-that-requested-cancellation",
			RequestId:  env.Tv().RequestID(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInput:   true,
			IncludeOutcome: true,
		})
		require.NoError(t, err)

		info := activityResp.GetInfo()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, info.GetStatus())
	})

	t.Run("StaleToken", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Fail with stale token (activity already completed)
		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {

		// Start an activity with retries, fail first attempt, then try to complete with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and get task token for attempt 1
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		// Fail the task with NextRetryDelay to control retry timing
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to get attempt 2 (ensures retry has happened)
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			RequestId:  env.Tv().RequestID(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		// Try to cancel with the old attempt 1 token - should fail with NotFound
		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))

		// Heartbeat then cancel with the attempt 2 token and should be no error
		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetCancelRequested(), "expected CancelRequested to be true but was false")

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Failure:   defaultFailure,
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedTokenNamespace", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to cancel with existing token
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})

	t.Run("NonExistent", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Reason:     "Test Cancellation",
			Identity:   "canceller",
		})

		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, fmt.Sprintf("activity not found for ID: %s", activityID), notFoundErr.Message)
	})
}

func (s *standaloneActivityTestSuite) TestTerminate() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("TerminatedSuccessfully", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		identity := "terminator"
		_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   identity,
		})
		require.NoError(t, err)

		activityResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          runID,
			IncludeInput:   true,
			IncludeOutcome: true,
		})

		info := activityResp.GetInfo()

		require.NoError(t, err)
		env.validateBaseActivityResponse(t, activityID, runID, activityResp)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, info.GetStatus(),
			"expected Terminated but is %s", info.GetStatus())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
			"expected Unspecified but is %s", info.GetRunState())
		require.EqualValues(t, 1, info.GetAttempt())
		require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0)) // Terminated doesn't set attempt completion, thus expect 0 here
		require.NotNil(t, info.GetCloseTime())
		require.Equal(t, env.Tv().WorkerIdentity(), info.GetLastWorkerIdentity())
		require.NotNil(t, info.GetLastStartedTime())
		require.Nil(t, info.GetLastFailure())

		expectedFailure := &failurepb.Failure{
			Message: "Test Termination",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
					Identity: identity,
				},
			},
		}
		protorequire.ProtoEqual(t, expectedFailure, activityResp.GetOutcome().GetFailure())
	})

	t.Run("AlreadyCompletedCannotTerminate", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
			Identity:  "new-worker",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "worker",
		})
		require.Error(t, err)

	})

	t.Run("DuplicateRequestIDSucceeds", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)
	})

	t.Run("DifferentRequestIDFails", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RequestId:  "test-request-id-2",
			RunId:      runID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("NonExistent", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Reason:     "Test Termination",
			Identity:   "terminator",
		})

		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.Equal(t, fmt.Sprintf("activity not found for ID: %s", activityID), notFoundErr.Message)
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyActivityID", func(t *testing.T) {
			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				Namespace: env.Namespace().String(),
				Reason:    "Test Termination",
				Identity:  "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  env.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("RequestIDTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RequestId:  string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
				Namespace:  env.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("request ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  env.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)), // dynamic config default is 1000
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Namespace:  env.Namespace().String(),
				Reason:     "Test Termination",
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})

		t.Run("ReasonTooLong", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := env.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
				ActivityId: testcore.RandomizeStr(t.Name()),
				Namespace:  env.Namespace().String(),
				Reason:     string(make([]byte, blobSizeLimitError+1)),
				Identity:   "terminator",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
		})
	})
}

func (env *standaloneActivityEnv) eventuallyTerminated(ctx context.Context, t *testing.T, activityID, runID string) {
	t.Helper()
	await.Require(ctx, t, func(c *await.T) {
		resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(c, err)
		require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, resp.GetInfo().GetStatus())
	}, 5*time.Second, 100*time.Millisecond)
}

func (env *standaloneActivityEnv) eventuallyTimedOut(ctx context.Context, t *testing.T, activityID, runID string) {
	t.Helper()
	await.Require(ctx, t, func(c *await.T) {
		resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(c, err)
		require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
	}, 10*time.Second, 100*time.Millisecond)
}

func (env *standaloneActivityEnv) eventuallyDeleted(ctx context.Context, t *testing.T, activityID, runID string) {
	t.Helper()
	await.Require(ctx, t, func(c *await.T) {
		_, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(c, err, &notFoundErr)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *standaloneActivityTestSuite) TestDelete() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("DeleteScheduledActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteRunningActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteCompletedActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteTerminatedActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		_ = env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			Identity:   defaultIdentity,
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		env.eventuallyTerminated(s.Context(), t, activityID, runID)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteCancelRequestedActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		_ = env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   defaultIdentity,
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteFailedActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Failure:   defaultFailure,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteTimedOutActivity", func(t *testing.T) {
		timedOutCtx, timedOutCancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer timedOutCancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(timedOutCtx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1,
			},
			RequestId: env.Tv().RequestID(),
		})
		require.NoError(t, err)
		runID := startResp.RunId

		_, err = env.FrontendClient().PollActivityTaskQueue(timedOutCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		env.eventuallyTimedOut(timedOutCtx, t, activityID, runID)

		_, err = env.FrontendClient().DeleteActivityExecution(timedOutCtx, &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(timedOutCtx, t, activityID, runID)
	})

	t.Run("DeleteDeletedActivity", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.ErrorContains(t, err, fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("DeleteActivityNoRunID", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
		runID := startResp.RunId

		pollTaskResp := env.pollActivityTaskAndValidate(s.Context(), t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)

		env.eventuallyDeleted(s.Context(), t, activityID, runID)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})

		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
		require.ErrorContains(t, err, fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("RequestValidations", func(t *testing.T) {
		t.Run("EmptyActivityID", func(t *testing.T) {

			_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
				Namespace: env.Namespace().String(),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {

			_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {

			_, err := env.FrontendClient().DeleteActivityExecution(s.Context(), &workflowservice.DeleteActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
			})

			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})
	})
}

func (s *standaloneActivityTestSuite) TestRetryWithoutScheduleToCloseTimeout() {
	env := s.newTestEnv()
	t := s.T()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	// Start activity without ScheduleToCloseTimeout
	_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(1 * time.Minute),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Millisecond),
			MaximumAttempts: 2,
		},
	})
	require.NoError(t, err)

	// Attempt 1: fail retryably
	pollResp1, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, pollResp1.Attempt)
	_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp1.TaskToken,
		Failure: &failurepb.Failure{
			Message: "retryable failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
			},
		},
	})
	require.NoError(t, err)

	// Attempt 2 should be scheduled
	pollResp2, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, pollResp2.Attempt)
}

func (s *standaloneActivityTestSuite) Test_ScheduleToCloseTimeout_WithRetry() {
	env := s.newTestEnv()
	t := s.T()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	// Start an activity
	startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		// It's not possible to guarantee (e.g. via NextRetryDelay or RetryPolicy) that a retry
		// will start with a delay <1s because of the use of TimerProcessorMaxTimeShift in the
		// timer queue. Therefore we allow 1s for the ActivityDispatchTask to be executed, and
		// time out the activity 1s into Attempt 2.
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
	})
	require.NoError(t, err)

	// Fail attempt 1, causing the attempt counter to increment.
	pollTaskResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
	require.NoError(t, err)
	_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Failure: &failurepb.Failure{
			Message: "Retryable failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable:   false,
				NextRetryDelay: durationpb.New(1 * time.Second),
			}},
		},
	})
	require.NoError(t, err)
	_, err = env.pollActivityTaskQueue(s.Context(), taskQueue)
	require.NoError(t, err)

	// Wait for schedule-to-close timeout.
	pollActivityResp, err := env.FrontendClient().PollActivityExecution(s.Context(), &workflowservice.PollActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, pollActivityResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected ScheduleToCloseTimeout but is %s", pollActivityResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())

	describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.Greater(t, describeResp.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, describeResp.GetInfo().GetCloseTime().AsTime().IsZero())
}

// TestStartToCloseTimeout tests that a start-to-close timeout is recorded after the activity is
// started. It also verifies that DescribeActivityExecution can be used to long-poll for a TimedOut
// state change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestStartToCloseTimeout() {
	env := s.newTestEnv()
	t := s.T()

	activityID := env.Tv().ActivityID()
	taskQueue := env.Tv().TaskQueue()

	startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		Input: payloads.EncodeString("test-activity-input"),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.Name,
		},
		StartToCloseTimeout: durationpb.New(1 * time.Second),
		// This test is expecting activity failure on start-to-close timeout.
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumAttempts: 1,
		},
		RequestId: "test-request-id",
	})
	require.NoError(t, err)

	// First poll: activity has not started yet
	describeResp1, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp1)
	require.NotNil(t, describeResp1.GetInfo())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp1.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp1.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, describeResp1.GetInfo().GetRunState(),
		"expected Scheduled but is %s", describeResp1.GetInfo().GetRunState())

	// Worker poll to start the activity
	pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.Name,
		},
		Identity: env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)
	require.NotNil(t, pollTaskResp)
	require.NotEmpty(t, pollTaskResp.TaskToken)

	// Second poll: activity has started
	describeResp2, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:     env.Namespace().String(),
		ActivityId:    activityID,
		RunId:         startResp.RunId,
		LongPollToken: describeResp1.LongPollToken,
	})
	require.NoError(t, err)
	require.NotNil(t, describeResp2)
	require.NotNil(t, describeResp2.GetInfo())
	require.Positive(t, describeResp2.GetInfo().GetStateSizeBytes())
	require.Greater(t, describeResp2.GetInfo().GetStateTransitionCount(), describeResp1.GetInfo().GetStateTransitionCount())
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, describeResp2.GetInfo().GetStatus(),
		"expected Running but is %s", describeResp2.GetInfo().GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, describeResp2.GetInfo().GetRunState(),
		"expected Started but is %s", describeResp2.GetInfo().GetRunState())

	// Third poll: activity has timed out
	describeResp3, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:          env.Namespace().String(),
		ActivityId:         activityID,
		RunId:              startResp.RunId,
		IncludeOutcome:     true,
		IncludeLastFailure: true,
		LongPollToken:      describeResp2.LongPollToken,
	})

	require.NoError(t, err)
	require.NotNil(t, describeResp3)
	require.NotNil(t, describeResp3.GetInfo())
	require.Positive(t, describeResp3.GetInfo().GetStateSizeBytes())
	require.Greater(t, describeResp3.GetInfo().GetStateTransitionCount(), describeResp2.GetInfo().GetStateTransitionCount())

	// The activity has timed out due to StartToClose. This is an attempt failure, therefore the
	// failure should be in ActivityExecutionInfo.LastFailure as well as set as the outcome failure.
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, describeResp3.GetInfo().GetStatus(),
		"expected TimedOut but is %s", describeResp3.GetInfo().GetStatus())
	require.Greater(t, describeResp3.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, describeResp3.GetInfo().GetCloseTime().AsTime().IsZero())
	failure := describeResp3.GetInfo().GetLastFailure()
	require.NotNil(t, failure)
	timeoutFailure := failure.GetTimeoutFailureInfo()
	require.NotNil(t, timeoutFailure)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutFailure.GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", timeoutFailure.GetTimeoutType())

	require.NotNil(t, describeResp3.GetOutcome().GetFailure())
	protorequire.ProtoEqual(t, failure, describeResp3.GetOutcome().GetFailure())
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, describeResp3.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected StartToCloseTimeout but is %s", describeResp3.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

// TestStartToCloseTimeout_WhileCancelRequested verifies that an activity
// times out due to start-to-close timeout, after a cancellation request has been accepted.
func (s *standaloneActivityTestSuite) TestStartToCloseTimeout_WhileCancelRequested() {
	env := s.newTestEnv()
	t := s.T()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(2 * time.Second),
		RetryPolicy:         &commonpb.RetryPolicy{MaximumAttempts: 1},
	})
	require.NoError(t, err)

	// Worker accepts the task — activity is STARTED.
	_, err = env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	require.NoError(t, err)

	// Request cancellation — activity moves to CANCEL_REQUESTED.
	_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		Identity:   "canceller",
		RequestId:  "cancel-req-1",
	})
	require.NoError(t, err)

	// Worker ignores cancellation and doesn't respond.
	// The start-to-close timeout (2s) should still fire.
	pollOutcome, err := env.FrontendClient().PollActivityExecution(s.Context(), &workflowservice.PollActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"activity in CANCEL_REQUESTED should still time out via START_TO_CLOSE")
}

// TestScheduleToStartTimeout tests that a schedule-to-start timeout is recorded after the activity is
// created but never started. It also verifies that DescribeActivityExecution can be used to long-poll for a TimedOut
// state change caused by execution of a timer task.
func (s *standaloneActivityTestSuite) TestScheduleToStartTimeout() {
	env := s.newTestEnv()
	t := s.T()

	activityID := env.Tv().ActivityID()
	taskQueue := env.Tv().TaskQueue()

	startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		Input: payloads.EncodeString("test-activity-input"),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue.Name,
		},
		ScheduleToStartTimeout: durationpb.New(100 * time.Millisecond),
		StartToCloseTimeout:    durationpb.New(30 * time.Second),
		RequestId:              "test-request-id",
	})
	require.NoError(t, err)

	describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	require.NoError(t, err)

	// Long poll to await activity timeout
	describeResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      env.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResp.RunId,
		IncludeOutcome: true,
		LongPollToken:  describeResp.LongPollToken,
	})

	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, describeResp.GetInfo().GetStatus(),
		"expected TimedOut but is %s", describeResp.GetInfo().GetStatus())
	require.Greater(t, describeResp.GetInfo().GetExecutionDuration().AsDuration(), time.Duration(0))
	require.NotNil(t, describeResp.GetInfo().GetCloseTime())
	require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		"expected ScheduleToStartTimeout but is %s", describeResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
}

func (s *standaloneActivityTestSuite) TestDescribeActivityExecution() {
	s.Run("NoWait", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := env.Tv().ActivityID()
		taskQueue := env.Tv().TaskQueue()

		startReq := &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Header:                 defaultHeader,
			HeartbeatTimeout:       durationpb.New(45 * time.Second),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			ScheduleToStartTimeout: durationpb.New(30 * time.Second),
			ScheduleToCloseTimeout: durationpb.New(3 * time.Minute),
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(2 * time.Second),
				BackoffCoefficient: 1.5,
				MaximumAttempts:    3,
				MaximumInterval:    durationpb.New(111 * time.Second),
			},
			Priority: &commonpb.Priority{
				FairnessKey: "test-key",
			},
			SearchAttributes: defaultSearchAttributes,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue.GetName(),
			},
			UserMetadata: defaultUserMetadata,
		}

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), startReq)
		require.NoError(t, err)

		t.Run("MinimalResponse", func(t *testing.T) {
			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				// Omit RunID to verify that latest run will be used
				IncludeInput:   false,
				IncludeOutcome: false,
			})
			require.NoError(t, err)
			require.NotNil(t, describeResp.LongPollToken)
			require.Equal(t, startResp.RunId, describeResp.RunId)
			require.Nil(t, describeResp.Input)
			require.Nil(t, describeResp.GetOutcome().GetResult())
			require.Nil(t, describeResp.GetOutcome().GetFailure())
		})

		t.Run("FullResponse", func(t *testing.T) {
			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:      env.Namespace().String(),
				ActivityId:     activityID,
				RunId:          startResp.RunId,
				IncludeInput:   true,
				IncludeOutcome: true,
			})
			require.NoError(t, err)

			respInfo := describeResp.GetInfo()
			require.NotNil(t, describeResp.LongPollToken)
			require.NotNil(t, respInfo)

			expectedExpirationTime := timestamppb.New(respInfo.GetScheduleTime().AsTime().Add(
				startReq.GetScheduleToCloseTimeout().AsDuration()))

			expected := &activitypb.ActivityExecutionInfo{
				ActivityId:             activityID,
				ActivityType:           env.Tv().ActivityType(),
				Attempt:                1,
				ExpirationTime:         expectedExpirationTime,
				Header:                 defaultHeader,
				HeartbeatTimeout:       startReq.GetHeartbeatTimeout(),
				RetryPolicy:            startReq.GetRetryPolicy(),
				RunId:                  startResp.RunId,
				RunState:               enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Priority:               startReq.GetPriority(),
				ScheduleToCloseTimeout: startReq.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout: startReq.GetScheduleToStartTimeout(),
				StartToCloseTimeout:    startReq.GetStartToCloseTimeout(),
				Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
				SearchAttributes:       defaultSearchAttributes,
				TaskQueue:              taskQueue.Name,
				UserMetadata:           defaultUserMetadata,
			}

			// Ignore non-deterministic fields. Validated separately.
			protorequire.ProtoEqual(t, expected, respInfo,
				protorequire.IgnoreFields(
					"execution_duration",
					"execution_time",
					"schedule_time",
					"state_size_bytes",
					"state_transition_count",
				),
			)
			require.Equal(t, respInfo.GetExecutionDuration().AsDuration(), time.Duration(0)) // Never completed, so expect 0
			require.Nil(t, describeResp.GetInfo().GetCloseTime())
			require.Positive(t, respInfo.GetScheduleTime().AsTime().Unix())
			require.Positive(t, respInfo.GetStateSizeBytes())
			require.Positive(t, respInfo.GetStateTransitionCount())

			protorequire.ProtoEqual(t, defaultInput, describeResp.Input)

			// Activity is scheduled but not completed, so no outcome yet
			require.Nil(t, describeResp.GetOutcome().GetResult())
			require.Nil(t, describeResp.GetOutcome().GetFailure())
		})
	})

	s.Run("StartDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := env.Tv().ActivityID()
		taskQueue := env.Tv().TaskQueue()
		startDelay := durationpb.New(300 * time.Second)

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue.GetName()},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          startDelay,
			RequestId:           env.Tv().RequestID(),
		})
		require.NoError(t, err)

		describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, startDelay, describeResp.GetInfo().GetStartDelay())
		expectedExecution := describeResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay.AsDuration())
		require.Equal(t, expectedExecution, describeResp.GetInfo().GetExecutionTime().AsTime(),
			"execution_time should equal scheduleTime + start_delay")
	})

	s.Run("SensitiveFieldsRequireOptIn", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.startActivity(s.Context(), activityID, taskQueue)
		require.NoError(t, err)

		pollTaskResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   defaultHeartbeatDetails,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Failure:   defaultFailure,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Nil(t, describeResp.GetInfo().GetHeartbeatDetails())
		require.Nil(t, describeResp.GetInfo().GetLastFailure())

		describeResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.RunId,
			IncludeHeartbeatDetails: true,
			IncludeLastFailure:      true,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, defaultHeartbeatDetails, describeResp.GetInfo().GetHeartbeatDetails())
		protorequire.ProtoEqual(t, defaultFailure, describeResp.GetInfo().GetLastFailure())
	})

	s.Run("WaitAnyStateChange", func(s *standaloneActivityTestSuite) {
		t := s.T()
		// Long poll for any state change. PollActivityTaskQueue is used to cause a state change.
		env := s.newTestEnv()

		activityID := env.Tv().ActivityID()
		taskQueue := env.Tv().TaskQueue()

		startResp, err := env.startActivity(s.Context(), activityID, taskQueue.Name)
		require.NoError(t, err)

		// First poll lacks token and therefore responds immediately, returning a token
		firstDescribeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			// RunId:        startResp.RunId, // RunID is now required by validation [?]
			IncludeInput: true,
		})
		require.NoError(t, err)
		require.NotNil(t, firstDescribeResp.LongPollToken)
		require.NotNil(t, firstDescribeResp.Info)
		require.Equal(t, firstDescribeResp.RunId, startResp.RunId)

		expected := &activitypb.ActivityExecutionInfo{
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Attempt:                1,
			HeartbeatTimeout:       durationpb.New(0),
			RetryPolicy:            defaultRetryPolicy,
			RunId:                  startResp.RunId,
			RunState:               enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			SearchAttributes:       &commonpb.SearchAttributes{},
			ScheduleToCloseTimeout: durationpb.New(0),
			ScheduleToStartTimeout: durationpb.New(0),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			TaskQueue:              taskQueue.Name,
		}

		// Ignore non-deterministic fields. Validated separately.
		protorequire.ProtoEqual(t, expected, firstDescribeResp.GetInfo(),
			protorequire.IgnoreFields(
				"execution_duration",
				"execution_time",
				"schedule_time",
				"state_size_bytes",
				"state_transition_count",
			),
		)
		require.Positive(t, firstDescribeResp.GetInfo().GetStateSizeBytes())

		taskQueuePollErr := make(chan error, 1)
		activityPollDone := make(chan struct{})
		var describeResp *workflowservice.DescribeActivityExecutionResponse
		var describeErr error

		go func() {
			defer close(activityPollDone)
			// Second poll uses token and therefore waits for a state transition
			describeResp, describeErr = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     env.Namespace().String(),
				ActivityId:    activityID,
				RunId:         startResp.RunId,
				IncludeInput:  true,
				LongPollToken: firstDescribeResp.LongPollToken,
			})
		}()

		// TODO(dan): race here: subscription might not be established yet

		// Worker picks up activity task, triggering transition (via RecordActivityTaskStarted)
		go func() {
			_, err := env.pollActivityTaskQueue(metadata.NewOutgoingContext(s.Context(), metadata.MD{}), taskQueue.Name)
			taskQueuePollErr <- err
		}()

		select {
		case <-activityPollDone:
			require.NoError(t, describeErr)
			require.NotNil(t, describeResp)
			require.NotNil(t, describeResp.Info)

			expected := &activitypb.ActivityExecutionInfo{
				ActivityId:             activityID,
				ActivityType:           env.Tv().ActivityType(),
				Attempt:                1,
				HeartbeatTimeout:       durationpb.New(0),
				LastWorkerIdentity:     defaultIdentity,
				RetryPolicy:            defaultRetryPolicy,
				RunId:                  startResp.RunId,
				RunState:               enumspb.PENDING_ACTIVITY_STATE_STARTED,
				ScheduleToCloseTimeout: durationpb.New(0),
				ScheduleToStartTimeout: durationpb.New(0),
				SearchAttributes:       &commonpb.SearchAttributes{},
				StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
				Status:                 enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
				TaskQueue:              taskQueue.Name,
			}

			// Ignore non-deterministic fields. Validated separately.
			protorequire.ProtoEqual(t, expected, describeResp.GetInfo(),
				protorequire.IgnoreFields(
					"execution_duration",
					"execution_time",
					"last_started_time",
					"schedule_time",
					"state_size_bytes",
					"state_transition_count",
				),
			)
			require.Positive(t, describeResp.GetInfo().GetStateSizeBytes())

			protorequire.ProtoEqual(t, defaultInput, describeResp.Input)

		case <-s.Context().Done():
			t.Fatal("DescribeActivityExecution timed out")
		}

		err = <-taskQueuePollErr
		require.NoError(t, err)
	})

	s.Run("ExposesSDKMetadata", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		t.Run("Populated", func(t *testing.T) {

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp, err := env.startActivity(s.Context(), activityID, taskQueue)
			require.NoError(t, err)

			pollCtx := metadata.NewOutgoingContext(s.Context(), metadata.Pairs(
				headers.ClientNameHeaderName, headers.ClientNameGoSDK,
				headers.ClientVersionHeaderName, temporal.SDKVersion,
			))
			_, err = env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: defaultIdentity,
			})
			require.NoError(t, err)

			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.Equal(t, headers.ClientNameGoSDK, describeResp.GetInfo().GetSdkName())
			require.Equal(t, temporal.SDKVersion, describeResp.GetInfo().GetSdkVersion())
		})

		t.Run("EmptyWhenAbsent", func(t *testing.T) {

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp, err := env.startActivity(s.Context(), activityID, taskQueue)
			require.NoError(t, err)

			pollCtx := metadata.NewOutgoingContext(s.Context(), metadata.MD{})
			_, err = env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: defaultIdentity,
			})
			require.NoError(t, err)

			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.Empty(t, describeResp.GetInfo().GetSdkName())
			require.Empty(t, describeResp.GetInfo().GetSdkVersion())
		})

		// When an attempt fails and a new attempt is scheduled, the next poller's SDK identity must overwrite the
		// previous attempt's values.
		t.Run("OverwrittenOnRetry", func(t *testing.T) {

			activityID := testcore.RandomizeStr(t.Name())
			taskQueue := testcore.RandomizeStr(t.Name())

			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:           env.Namespace().String(),
				ActivityId:          activityID,
				ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				StartToCloseTimeout: durationpb.New(1 * time.Minute),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
					MaximumAttempts: 2,
				},
			})
			require.NoError(t, err)

			// Attempt 1: poll as the Go SDK, then fail retryably.
			pollCtx1 := metadata.NewOutgoingContext(s.Context(), metadata.Pairs(
				headers.ClientNameHeaderName, headers.ClientNameGoSDK,
				headers.ClientVersionHeaderName, temporal.SDKVersion,
			))
			pollResp1, err := env.FrontendClient().PollActivityTaskQueue(pollCtx1, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Identity:  defaultIdentity,
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, pollResp1.Attempt)

			// Sanity-check that attempt 1's Go SDK identity is set before the retry overwrites it.
			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(t, err)
			require.Equal(t, headers.ClientNameGoSDK, describeResp.GetInfo().GetSdkName())
			require.Equal(t, temporal.SDKVersion, describeResp.GetInfo().GetSdkVersion())

			_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: pollResp1.TaskToken,
				Failure: &failurepb.Failure{
					Message: "retryable failure",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
					},
				},
				Identity: defaultIdentity,
			})
			require.NoError(t, err)

			// Attempt 2: poll as a different SDK to prove the value is overwritten, not appended.
			// Java version is hardcoded since we don't have the Java SDK imported.
			javaSdkVersion := "1.35.0"
			pollCtx2 := metadata.NewOutgoingContext(s.Context(), metadata.Pairs(
				headers.ClientNameHeaderName, headers.ClientNameJavaSDK,
				headers.ClientVersionHeaderName, javaSdkVersion,
			))
			pollResp2, err := env.FrontendClient().PollActivityTaskQueue(pollCtx2, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Identity:  defaultIdentity,
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, pollResp2.Attempt)

			describeResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(t, err)
			require.Equal(t, headers.ClientNameJavaSDK, describeResp.GetInfo().GetSdkName())
			require.Equal(t, javaSdkVersion, describeResp.GetInfo().GetSdkVersion())
			require.EqualValues(t, 2, describeResp.GetInfo().GetAttempt())
		})
	})

	s.Run("Completed", func(s *standaloneActivityTestSuite) {
		env := s.newTestEnv()
		testCases := []struct {
			name             string
			expectedStatus   enumspb.ActivityExecutionStatus
			taskCompletionFn func(ctx context.Context, taskToken []byte, activityID, runID string) error
			outcomeValidator func(*testing.T, *workflowservice.DescribeActivityExecutionResponse)
		}{
			{
				name:           "successful completion",
				expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
					_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
						Namespace: env.Namespace().String(),
						TaskToken: taskToken,
						Result:    defaultResult,
						Identity:  defaultIdentity,
					})
					return err
				},
				outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
					protorequire.ProtoEqual(t, defaultResult, response.GetOutcome().GetResult())
					require.Nil(t, response.GetOutcome().GetFailure())
				},
			},
			{
				name:           "failure completion",
				expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
					_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
						Namespace: env.Namespace().String(),
						TaskToken: taskToken,
						Failure:   defaultFailure,
						Identity:  defaultIdentity,
					})
					return err
				},
				outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
					protorequire.ProtoEqual(t, defaultFailure, response.GetOutcome().GetFailure())
					require.Nil(t, response.GetOutcome().GetResult())
				},
			},
			{
				name:           "termination",
				expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
				taskCompletionFn: func(ctx context.Context, _ []byte, activityID, runID string) error {
					_, err := env.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
						Namespace:  env.Namespace().String(),
						ActivityId: activityID,
						RunId:      runID,
						Reason:     "test termination",
					})
					return err
				},
				outcomeValidator: func(t *testing.T, response *workflowservice.DescribeActivityExecutionResponse) {
					require.NotNil(t, response.GetOutcome().GetFailure())
					require.NotNil(t, response.GetOutcome().GetFailure().GetTerminatedFailureInfo())
					require.Nil(t, response.GetOutcome().GetResult())
				},
			},
		}

		for _, tc := range testCases {
			s.Run(tc.name, func(s *standaloneActivityTestSuite) {
				t := s.T()

				activityID := env.Tv().Any().String()
				taskQueue := &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr(t.Name())}

				startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
					Namespace:           env.Namespace().String(),
					ActivityId:          activityID,
					ActivityType:        env.Tv().ActivityType(),
					Header:              defaultHeader,
					HeartbeatTimeout:    durationpb.New(45 * time.Second),
					Identity:            env.Tv().WorkerIdentity(),
					Input:               defaultInput,
					StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
					RequestId:           env.Tv().RequestID(),
					RetryPolicy:         defaultRetryPolicy,
					Priority:            &commonpb.Priority{FairnessKey: "test-key"},
					SearchAttributes:    defaultSearchAttributes,
					TaskQueue:           taskQueue,
					UserMetadata:        defaultUserMetadata,
				})
				require.NoError(t, err)

				pollTaskResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue.Name)
				require.NoError(t, err)
				err = tc.taskCompletionFn(s.Context(), pollTaskResp.TaskToken, activityID, startResp.RunId)
				require.NoError(t, err)

				describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
					Namespace:      env.Namespace().String(),
					ActivityId:     activityID,
					RunId:          startResp.RunId,
					IncludeInput:   true,
					IncludeOutcome: true,
				})
				require.NoError(t, err)
				require.NotNil(t, describeResp)
				require.Equal(t, startResp.RunId, describeResp.GetRunId())
				protorequire.ProtoEqual(t, defaultInput, describeResp.GetInput())
				// info fields
				info := describeResp.GetInfo()
				require.NotNil(t, info)
				require.Equal(t, activityID, info.GetActivityId())
				require.Equal(t, startResp.RunId, info.GetRunId())
				protorequire.ProtoEqual(t, env.Tv().ActivityType(), info.GetActivityType())
				require.Equal(t, tc.expectedStatus, info.GetStatus())
				require.Equal(t, taskQueue.Name, info.GetTaskQueue())
				require.Equal(t, int32(1), info.GetAttempt())
				protorequire.ProtoEqual(t, defaultRetryPolicy, info.GetRetryPolicy())
				protorequire.ProtoEqual(t, defaultHeader, info.GetHeader())
				protorequire.ProtoEqual(t, &commonpb.Priority{FairnessKey: "test-key"}, info.GetPriority())
				protorequire.ProtoEqual(t, defaultSearchAttributes, info.GetSearchAttributes())
				protorequire.ProtoEqual(t, defaultUserMetadata, info.GetUserMetadata())
				require.Equal(t, 45*time.Second, info.GetHeartbeatTimeout().AsDuration())
				require.Equal(t, defaultStartToCloseTimeout, info.GetStartToCloseTimeout().AsDuration())
				require.Equal(t, defaultIdentity, info.GetLastWorkerIdentity())
				// time fields
				require.NotNil(t, info.GetScheduleTime())
				require.Positive(t, info.GetScheduleTime().AsTime().Unix())
				require.NotNil(t, info.GetLastStartedTime())
				require.Positive(t, info.GetLastStartedTime().AsTime().Unix())
				require.NotNil(t, info.GetCloseTime())
				require.Positive(t, info.GetCloseTime().AsTime().Unix())
				require.GreaterOrEqual(t, info.GetCloseTime().AsTime().UnixNano(), info.GetLastStartedTime().AsTime().UnixNano())
				require.Positive(t, info.GetStateSizeBytes())
				require.Positive(t, info.GetStateTransitionCount())

				tc.outcomeValidator(t, describeResp)
			})
		}
	})

	s.Run("DeadlineExceeded", func(s *standaloneActivityTestSuite) {
		env := s.newTestEnv()
		t := s.T()
		ctx := s.Context()

		// Start an activity and get initial long-poll state token
		activityID := env.Tv().ActivityID()
		taskQueue := env.Tv().TaskQueue()
		startResp, err := env.startActivity(ctx, activityID, taskQueue.Name)
		require.NoError(t, err)
		describeResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)

		// The DescribeActivityExecution calls below use a long-poll token and will necessarily time out,
		// because the activity undergoes no further state transitions.

		// The timeout imposed by the server is essentially
		// Min(CallerTimeout - LongPollBuffer, LongPollTimeout)

		// Case 1: Caller sets a deadline which has room for the buffer. History returns empty success
		// result with at least buffer remaining before the caller deadline.
		t.Run("CallerDeadlineNotExceeded", func(t *testing.T) {
			// CallerTimeout - LongPollBuffer is far in the future
			cleanup1 := env.OverrideDynamicConfig(activity.LongPollBuffer, 1*time.Second)
			defer cleanup1()
			ctx, cancel := context.WithTimeout(ctx, 9999*time.Millisecond)
			defer cancel()

			// DescribeActivityExecution will return when this long poll timeout expires.
			cleanup2 := env.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)
			defer cleanup2()

			describeResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     env.Namespace().String(),
				ActivityId:    activityID,
				RunId:         startResp.RunId,
				LongPollToken: describeResp.LongPollToken,
			})
			// The server uses an empty non-error response to indicate to the caller that it should resubmit
			// its long-poll.
			require.NoError(t, err)
			require.Empty(t, describeResp.GetInfo())
		})

		// Case 2: caller does not set a deadline. In practice this is equivalent to them setting a 30s
		// deadline since that is what Histry receives. In this case History times out the wait at
		// LongPollTimeout and the caller gets an empty response.
		t.Run("NoCallerDeadline", func(t *testing.T) {
			// The caller sets no deadline. However, the ctx received by the history service handler
			// will have a 30s deadline that was applied by one of the upstream server layers, so we
			// still must use a buffer < 30s.
			ctx := context.Background()
			cleanup1 := env.OverrideDynamicConfig(activity.LongPollBuffer, 29*time.Second)
			defer cleanup1()
			// DescribeActivityExecution will return when this long poll timeout expires.
			cleanup2 := env.OverrideDynamicConfig(activity.LongPollTimeout, 10*time.Millisecond)
			defer cleanup2()

			_, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     env.Namespace().String(),
				ActivityId:    activityID,
				RunId:         startResp.RunId,
				LongPollToken: describeResp.LongPollToken,
			})
			require.NoError(t, err)
			require.Empty(t, describeResp.GetInfo())
		})

		// Case 3: caller sets a deadline that is < the buffer. In this case DescribeActivityExecution
		// will return an empty result immediately, and there is a race between caller receiving that
		// and caller's client timing out the request. Therefore we do not test this.
	})

	s.Run("NotFound", func(s *standaloneActivityTestSuite) {
		env := s.newTestEnv()
		t := s.T()
		ctx := s.Context()

		existingActivityID := env.Tv().ActivityID()
		tq := env.Tv().TaskQueue()
		startResp, err := env.startActivity(ctx, existingActivityID, tq.Name)
		require.NoError(t, err)
		existingRunID := startResp.RunId
		require.NotEmpty(t, existingRunID)
		existingNamespace := env.Namespace().String()

		var notFoundErr *serviceerror.NotFound
		var namespaceNotFoundErr *serviceerror.NamespaceNotFound

		testCases := []struct {
			name           string
			request        *workflowservice.DescribeActivityExecutionRequest
			expectedErr    error
			expectedErrMsg string
		}{
			{
				name: "NonExistentNamespace",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  "non-existent-namespace",
					ActivityId: existingActivityID,
					RunId:      existingRunID,
				},
				expectedErr:    namespaceNotFoundErr,
				expectedErrMsg: "Namespace non-existent-namespace is not found.",
			},
			{
				name: "NonExistentActivityID",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: "non-existent-activity",
					RunId:      existingRunID,
				},
				expectedErr:    notFoundErr,
				expectedErrMsg: "activity not found for ID: non-existent-activity",
			},
			{
				name: "NonExistentRunID",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: existingActivityID,
					RunId:      "11111111-2222-3333-4444-555555555555",
				},
				expectedErr:    notFoundErr,
				expectedErrMsg: fmt.Sprintf("activity not found for ID: %s", existingActivityID),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := env.FrontendClient().DescribeActivityExecution(ctx, tc.request)
				require.ErrorAs(t, err, &tc.expectedErr) //nolint:testifylint
				require.Equal(t, tc.expectedErrMsg, tc.expectedErr.Error())
			})
		}

		t.Run("LongPollNonExistentActivity", func(t *testing.T) {
			// Poll to get a token
			validPollResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			})
			require.NoError(t, err)

			// Use the token with a non-existent activity
			_, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     existingNamespace,
				ActivityId:    "non-existent-activity",
				RunId:         existingRunID,
				LongPollToken: validPollResp.LongPollToken,
			})
			var notFoundErr *serviceerror.NotFound
			require.ErrorAs(t, err, &notFoundErr)
			require.Equal(t, "activity not found for ID: non-existent-activity", notFoundErr.Message)
		})
	})

	s.Run("InvalidArgument", func(s *standaloneActivityTestSuite) {
		env := s.newTestEnv()
		t := s.T()
		ctx := s.Context()

		existingActivityID := env.Tv().ActivityID()
		tq := env.Tv().TaskQueue()
		startResp, err := env.startActivity(ctx, existingActivityID, tq.Name)
		require.NoError(t, err)
		existingRunID := startResp.RunId
		require.NotEmpty(t, existingRunID)
		existingNamespace := env.Namespace().String()

		validActivityID := "activity-id"
		validRunID := "11111111-2222-3333-4444-555555555555"

		testCases := []struct {
			name        string
			request     *workflowservice.DescribeActivityExecutionRequest
			expectedErr string
		}{
			{
				name: "EmptyNamespace",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  "",
					ActivityId: validActivityID,
					RunId:      validRunID,
				},
				expectedErr: "Namespace is empty",
			},
			{
				name: "EmptyActivityID",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: "",
					RunId:      validRunID,
				},
				expectedErr: "activity ID is required",
			},
			{
				name: "ActivityIDTooLong",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: string(make([]byte, 2000)),
					RunId:      validRunID,
				},
				expectedErr: "activity ID exceeds length limit",
			},
			{
				name: "InvalidRunID",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: validActivityID,
					RunId:      "invalid-uuid",
				},
				expectedErr: "invalid run id",
			},
			{
				name: "RunIdNotRequiredWhenWaitPolicyAbsent",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: existingActivityID,
					RunId:      "",
				},
				expectedErr: "",
			},
			{
				name: "RunIdNotRequiredWhenLongPollTokenAbsent",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:  existingNamespace,
					ActivityId: existingActivityID,
					RunId:      "",
				},
				expectedErr: "",
			},
			{
				name: "RunIdRequiredWhenLongPollTokenPresent",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:     existingNamespace,
					ActivityId:    validActivityID,
					RunId:         "",
					LongPollToken: []byte("doesn't-matter"),
				},
				expectedErr: "run id is required",
			},
			{
				name: "MalformedLongPollToken",
				request: &workflowservice.DescribeActivityExecutionRequest{
					Namespace:     existingNamespace,
					ActivityId:    existingActivityID,
					RunId:         existingRunID,
					LongPollToken: []byte("invalid-token"),
				},
				expectedErr: "invalid long poll token",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := env.FrontendClient().DescribeActivityExecution(ctx, tc.request)
				if tc.expectedErr == "" {
					require.NoError(t, err)
					return
				}
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, invalidArgErr.Message, tc.expectedErr)
			})
		}

		t.Run("LongPollTokenFromWrongExecution", func(t *testing.T) {
			validPollResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, validPollResp.LongPollToken)

			activityID2 := env.Tv().Any().String()
			startResp2, err := env.startActivity(ctx, activityID2, tq.Name)
			require.NoError(t, err)
			require.NotEmpty(t, startResp2.GetRunId())

			_, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     existingNamespace,
				ActivityId:    activityID2,
				RunId:         startResp2.GetRunId(),
				LongPollToken: validPollResp.LongPollToken,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "long poll token does not match execution", invalidArgErr.Message)
		})

		t.Run("LongPollTokenFromDifferentNamespace", func(t *testing.T) {
			// Get a valid poll token from activity in main namespace
			validPollResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			})
			require.NoError(t, err)
			require.NotEmpty(t, validPollResp.LongPollToken)

			// Start an activity in a different namespace
			externalNamespace := env.ExternalNamespace().String()
			externalActivityID := env.Tv().Any().String()
			externalStartResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:    externalNamespace,
				ActivityId:   externalActivityID,
				ActivityType: env.Tv().ActivityType(),
				Identity:     env.Tv().WorkerIdentity(),
				Input:        defaultInput,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: tq.Name,
				},
				StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
				RequestId:           env.Tv().Any().String(),
			})
			require.NoError(t, err)
			require.NotEmpty(t, externalStartResp.GetRunId())

			// Try to use main namespace's poll token with external namespace's activity
			_, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:     externalNamespace,
				ActivityId:    externalActivityID,
				RunId:         externalStartResp.GetRunId(),
				LongPollToken: validPollResp.LongPollToken,
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "long poll token does not match execution", invalidArgErr.Message)
		})
	})
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution() {
	env := s.newTestEnv()
	testCases := []struct {
		name                   string
		taskCompletionFn       func(ctx context.Context, taskToken []byte, activityID, runID string) error
		completionValidationFn func(*testing.T, *workflowservice.PollActivityExecutionResponse)
	}{
		{
			name: "successful completion",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
				_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: taskToken,
					Result:    defaultResult,
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultResult, response.GetOutcome().GetResult())
			},
		},
		{
			name: "failure completion",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, _, _ string) error {
				_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: taskToken,
					Failure:   defaultFailure,
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				protorequire.ProtoEqual(t, defaultFailure, response.GetOutcome().GetFailure())
			},
		},
		{
			name: "cancellation",
			taskCompletionFn: func(ctx context.Context, taskToken []byte, activityID, runID string) error {
				_, err := env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
				})
				if err != nil {
					return err
				}
				_, err = env.FrontendClient().RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
					Namespace: env.Namespace().String(),
					TaskToken: taskToken,
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				require.NotNil(t, response.GetOutcome().GetFailure())
				require.NotNil(t, response.GetOutcome().GetFailure().GetCanceledFailureInfo())
			},
		},
		{
			name: "termination",
			taskCompletionFn: func(ctx context.Context, _ []byte, activityID, runID string) error {
				_, err := env.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: activityID,
					RunId:      runID,
					Reason:     "test termination",
				})
				return err
			},
			completionValidationFn: func(t *testing.T, response *workflowservice.PollActivityExecutionResponse) {
				require.NotNil(t, response.GetOutcome().GetFailure())
				require.NotNil(t, response.GetOutcome().GetFailure().GetTerminatedFailureInfo())
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *standaloneActivityTestSuite) {
			t := s.T()

			activityID := env.Tv().Any().String()
			taskQueue := testcore.RandomizeStr(t.Name())

			startResp, err := env.startActivity(s.Context(), activityID, taskQueue)
			require.NoError(t, err)
			pollTaskResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
			require.NoError(t, err)
			err = tc.taskCompletionFn(s.Context(), pollTaskResp.TaskToken, activityID, startResp.RunId)
			require.NoError(t, err)
			pollActivityResp, err := env.FrontendClient().PollActivityExecution(s.Context(), &workflowservice.PollActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.NotNil(t, pollActivityResp)
			require.Equal(t, startResp.RunId, pollActivityResp.GetRunId())
			tc.completionValidationFn(t, pollActivityResp)
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_EmptyRunID() {
	env := s.newTestEnv()
	t := s.T()

	activityID := env.Tv().Any().String()
	taskQueue := env.Tv().TaskQueue().String()

	startResp, err := env.startActivity(s.Context(), activityID, taskQueue)
	require.NoError(t, err)

	pollTaskResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
	require.NoError(t, err)

	_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollTaskResp.TaskToken,
		Result:    defaultResult,
	})
	require.NoError(t, err)

	pollActivityResp, err := env.FrontendClient().PollActivityExecution(s.Context(), &workflowservice.PollActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      "", // resolves to current run ID
	})
	require.NoError(t, err)
	require.NotNil(t, pollActivityResp)
	require.Equal(t, startResp.RunId, pollActivityResp.GetRunId())
	protorequire.ProtoEqual(t, defaultResult, pollActivityResp.GetOutcome().GetResult())
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_NotFound() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	existingActivityID := env.Tv().ActivityID()
	tq := env.Tv().TaskQueue()
	startResp, err := env.startActivity(ctx, existingActivityID, tq.Name)
	require.NoError(t, err)
	existingRunID := startResp.RunId
	require.NotEmpty(t, existingRunID)
	existingNamespace := env.Namespace().String()

	var notFoundErr *serviceerror.NotFound
	var namespaceNotFoundErr *serviceerror.NamespaceNotFound

	testCases := []struct {
		name           string
		request        *workflowservice.PollActivityExecutionRequest
		expectedErr    error
		expectedErrMsg string
	}{
		{
			name: "NonExistentNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "non-existent-namespace",
				ActivityId: existingActivityID,
				RunId:      existingRunID,
			},
			expectedErr:    namespaceNotFoundErr,
			expectedErrMsg: "Namespace non-existent-namespace is not found.",
		},
		{
			name: "NonExistentActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "non-existent-activity",
				RunId:      existingRunID,
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: "activity not found for ID: non-existent-activity",
		},
		{
			name: "NonExistentRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: existingActivityID,
				RunId:      "11111111-2222-3333-4444-555555555555",
			},
			expectedErr:    notFoundErr,
			expectedErrMsg: fmt.Sprintf("activity not found for ID: %s", existingActivityID),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.FrontendClient().PollActivityExecution(ctx, tc.request)
			require.ErrorAs(t, err, &tc.expectedErr) //nolint:testifylint
			require.Equal(t, tc.expectedErrMsg, tc.expectedErr.Error())
		})
	}
}

func (s *standaloneActivityTestSuite) TestPollActivityExecution_InvalidArgument() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	existingNamespace := env.Namespace().String()
	validRunID := "11111111-2222-3333-4444-555555555555"

	testCases := []struct {
		name        string
		request     *workflowservice.PollActivityExecutionRequest
		expectedErr string
	}{
		{
			name: "EmptyNamespace",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  "",
				ActivityId: "activity-id",
				RunId:      validRunID,
			},
			expectedErr: "Namespace is empty",
		},
		{
			name: "EmptyActivityID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "",
				RunId:      validRunID,
			},
			expectedErr: "activity ID is required",
		},
		{
			name: "ActivityIDTooLong",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: string(make([]byte, 2000)),
				RunId:      validRunID,
			},
			expectedErr: "activity ID exceeds length limit",
		},
		{
			name: "InvalidRunID",
			request: &workflowservice.PollActivityExecutionRequest{
				Namespace:  existingNamespace,
				ActivityId: "activity-id",
				RunId:      "invalid-uuid",
			},
			expectedErr: "invalid run id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.FrontendClient().PollActivityExecution(ctx, tc.request)
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, invalidArgErr.Message, tc.expectedErr)
		})
	}
}

// TODO(dan): add tests that DescribeActivityExecution can wait for deletion, termination, cancellation etc

func (s *standaloneActivityTestSuite) TestListActivityExecutions() {
	env := s.newTestEnv()
	t := s.T()

	activityID := env.Tv().ActivityID()
	activityType := env.Tv().ActivityType().GetName()
	taskQueue := env.Tv().TaskQueue().GetName()
	startResp := env.startAndValidateActivity(s.Context(), t, activityID, taskQueue)
	runID := startResp.RunId

	verifyListQuery := func(t *testing.T, query string, pageSize int32) {
		t.Helper()
		var resp *workflowservice.ListActivityExecutionsResponse
		await.Require(
			s.Context(),
			t,
			func(c *await.T) {
				var err error
				resp, err = env.FrontendClient().ListActivityExecutions(c.Context(), &workflowservice.ListActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					PageSize:  pageSize,
					Query:     query,
				})
				require.NoError(c, err)
				require.NotEmpty(c, resp.GetExecutions())
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1, "expected exactly 1 result for query: %s", query)
		exec := resp.GetExecutions()[0]
		// Verify all ActivityExecutionListInfo fields
		require.Equal(t, activityID, exec.GetActivityId())
		require.Equal(t, runID, exec.GetRunId())
		require.Equal(t, activityType, exec.GetActivityType().GetName())
		require.Equal(t, taskQueue, exec.GetTaskQueue())
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, exec.GetStatus())
		require.NotNil(t, exec.GetScheduleTime())
		require.NotNil(t, exec.GetExecutionTime())
		require.Nil(t, exec.GetCloseTime())         // Running activity has no close time
		require.Nil(t, exec.GetExecutionDuration()) // Running activity has no execution duration
		require.GreaterOrEqual(t, exec.GetStateSizeBytes(), int64(0))
		require.GreaterOrEqual(t, exec.GetStateTransitionCount(), int64(0))
	}

	t.Run("QueryByActivityId", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 10)
	})

	t.Run("QueryByActivityType", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityType = '%s'", activityType), 10)
	})

	t.Run("QueryByExecutionStatus", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType), 10)
	})

	t.Run("QueryPausedActivity", func(t *testing.T) {
		pausedActivityID := testcore.RandomizeStr(t.Name())
		pausedActivityType := testcore.RandomizeStr(t.Name())
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          pausedActivityID,
			ActivityType:        &commonpb.ActivityType{Name: pausedActivityType},
			Identity:            env.Tv().WorkerIdentity(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           env.Tv().RequestID(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: pausedActivityID,
			RunId:      startResp.GetRunId(),
			Identity:   env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		var resp *workflowservice.ListActivityExecutionsResponse
		s.AwaitTrue(
			func() bool {
				resp, err = env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					PageSize:  10,
					Query:     fmt.Sprintf("ExecutionStatus = 'Paused' AND ActivityId = '%s'", pausedActivityID),
				})
				return err == nil && len(resp.GetExecutions()) == 1
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED, resp.GetExecutions()[0].GetStatus())

		var allResp *workflowservice.ListActivityExecutionsResponse
		s.AwaitTrue(
			func() bool {
				allResp, err = env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					PageSize:  10,
				})
				for _, execution := range allResp.GetExecutions() {
					if execution.GetActivityId() == pausedActivityID {
						return execution.GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED
					}
				}
				return false
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})

	t.Run("QueryByTaskQueue", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("TaskQueue = '%s' AND ActivityType = '%s'", taskQueue, activityType), 10)
	})

	t.Run("QueryByMultipleFields", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s' AND ActivityType = '%s'", activityID, activityType), 10)
	})

	// QueryByExecutionTime verifies delayed activities are queryable and countable by dispatch time,
	// List returns the Describe execution time, and a later boundary excludes the activity.
	t.Run("QueryByExecutionTime", func(t *testing.T) {
		delayedID := "execution-time-activity-id"
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          delayedID,
			ActivityType:        &commonpb.ActivityType{Name: activityType},
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			StartDelay:          durationpb.New(10 * time.Minute),
			RequestId:           "execution-time-request-id",
		})
		require.NoError(t, err)
		describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: delayedID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.NotNil(t, describeResp.GetInfo().GetScheduleTime())
		require.NotNil(t, describeResp.GetInfo().GetExecutionTime())
		// Describe and List source schedule time from separate snapshots, so they may differ slightly.
		scheduleTime := describeResp.GetInfo().GetScheduleTime().AsTime()
		expectedExecutionTime := describeResp.GetInfo().GetExecutionTime().AsTime()

		// Use the midpoint of the delay so dispatch time matches while creation time does not.
		inWindow := scheduleTime.Add(5 * time.Minute).Format(time.RFC3339)
		matchQuery := fmt.Sprintf("ActivityId = '%s' AND ExecutionTime > '%s'", delayedID, inWindow)

		var resp *workflowservice.ListActivityExecutionsResponse
		await.Require(
			s.Context(),
			t,
			func(c *await.T) {
				var err error
				resp, err = env.FrontendClient().ListActivityExecutions(c.Context(), &workflowservice.ListActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					PageSize:  10,
					Query:     matchQuery,
				})
				require.NoError(c, err)
				require.NotEmpty(c, resp.GetExecutions())
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1)
		exec := resp.GetExecutions()[0]
		require.Equal(t, delayedID, exec.GetActivityId())
		require.NotNil(t, exec.GetExecutionTime())
		require.WithinDuration(t, expectedExecutionTime, exec.GetExecutionTime().AsTime(), time.Millisecond)

		countResp, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     matchQuery,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), countResp.GetCount())

		pastWindow := expectedExecutionTime.Add(time.Second).Format(time.RFC3339Nano)
		noMatch, err := env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     fmt.Sprintf("ActivityId = '%s' AND ExecutionTime > '%s'", delayedID, pastWindow),
		})
		require.NoError(t, err)
		require.Empty(t, noMatch.GetExecutions())
	})

	// ExecutionTimeTracksStartDelayUpdates verifies that visibility recomputes ExecutionTime when
	// start delay changes and restores the original indexed value after a reset restores the options.
	t.Run("ExecutionTimeTracksStartDelayUpdates", func(t *testing.T) {
		activityID := "execution-time-update-activity-id"
		originalDelay := 10 * time.Minute
		updatedDelay := 2 * time.Minute
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: activityType},
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			StartDelay:          durationpb.New(originalDelay),
			RequestId:           "execution-time-update-request-id",
		})
		require.NoError(t, err)

		listExecution := func(ctx context.Context) (*activitypb.ActivityExecutionListInfo, error) {
			resp, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
				Namespace: env.Namespace().String(),
				PageSize:  10,
				Query:     fmt.Sprintf("ActivityId = '%s'", activityID),
			})
			if err != nil || len(resp.GetExecutions()) != 1 {
				return nil, err
			}
			return resp.GetExecutions()[0], nil
		}

		var originalExecutionTime time.Time
		await.Require(s.Context(), t, func(c *await.T) {
			exec, err := listExecution(c.Context())
			require.NoError(c, err)
			require.NotNil(c, exec)
			require.NotNil(c, exec.GetExecutionTime())
			originalExecutionTime = exec.GetExecutionTime().AsTime()
			require.False(c, originalExecutionTime.IsZero())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.GetRunId(),
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(updatedDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		await.Require(s.Context(), t, func(c *await.T) {
			exec, err := listExecution(c.Context())
			require.NoError(c, err)
			require.NotNil(c, exec)
			require.NotNil(c, exec.GetExecutionTime())
			require.Equal(c, originalExecutionTime.Add(updatedDelay-originalDelay), exec.GetExecutionTime().AsTime())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)
		await.Require(s.Context(), t, func(c *await.T) {
			exec, err := listExecution(c.Context())
			require.NoError(c, err)
			require.NotNil(c, exec)
			require.NotNil(c, exec.GetExecutionTime())
			require.Equal(c, originalExecutionTime, exec.GetExecutionTime().AsTime())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	t.Run("QueryByCustomSearchAttribute", func(t *testing.T) {
		customSAName := "CustomKeywordField"
		customSAValue := "custom-sa-test-value"
		customSAActivityID := "custom-sa-activity-id"

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          customSAActivityID,
			ActivityType:        &commonpb.ActivityType{Name: "custom-sa-activity-type"},
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().GetName()},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			RequestId:           env.Tv().RequestID(),
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					customSAName: sadefs.MustEncodeValue(customSAValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
				},
			},
		})
		require.NoError(t, err)

		var resp *workflowservice.ListActivityExecutionsResponse
		await.Require(
			s.Context(),
			t,
			func(c *await.T) {
				var err error
				resp, err = env.FrontendClient().ListActivityExecutions(c.Context(), &workflowservice.ListActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					PageSize:  10,
					Query:     fmt.Sprintf("%s = '%s'", customSAName, customSAValue),
				})
				require.NoError(c, err)
				require.NotEmpty(c, resp.GetExecutions())
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		require.Len(t, resp.GetExecutions(), 1)
		exec := resp.GetExecutions()[0]
		require.Equal(t, customSAActivityID, exec.GetActivityId())
		require.NotNil(t, exec.GetSearchAttributes())
		returnedSA := exec.GetSearchAttributes().GetIndexedFields()[customSAName]
		require.NotNil(t, returnedSA)
		var returnedValue string
		require.NoError(t, payload.Decode(returnedSA, &returnedValue))
		require.Equal(t, customSAValue, returnedValue)
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, err := env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     "invalid query syntax !!!",
		})
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		_, err := env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     "NonExistentField = 'value'",
		})
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		_, err := env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace: "non-existent-namespace",
			PageSize:  10,
			Query:     "",
		})
		var namespaceNotFound *serviceerror.NamespaceNotFound
		require.ErrorAs(t, err, &namespaceNotFound)
	})

	t.Run("ZeroPageSizeDefaultsToConfigMax", func(t *testing.T) {
		verifyListQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 0)
	})

	t.Run("ExceededPageSizeIsCapped", func(t *testing.T) {
		maxPageSize := int32(1)
		cleanup := env.OverrideDynamicConfig(
			dynamicconfig.FrontendVisibilityMaxPageSize,
			maxPageSize,
		)
		defer cleanup()

		testActivityType := testcore.RandomizeStr(t.Name())

		// Start multiple activities of the same type
		for range 2 {
			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:           env.Namespace().String(),
				ActivityId:          testcore.RandomizeStr(t.Name()),
				ActivityType:        &commonpb.ActivityType{Name: testActivityType},
				Identity:            env.Tv().WorkerIdentity(),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
				},
				RequestId: env.Tv().RequestID(),
			})
			require.NoError(t, err)
		}

		// Wait for both activities to be indexed in Elasticsearch before testing pagination
		await.Require(
			s.Context(),
			t,
			func(c *await.T) {
				countResp, err := env.FrontendClient().CountActivityExecutions(c.Context(), &workflowservice.CountActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     fmt.Sprintf("ActivityType = '%s'", testActivityType),
				})
				require.NoError(c, err)
				require.Equal(c, int64(2), countResp.GetCount())
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)

		// Get first page. Use pageSize > FrontendVisibilityMaxPageSize to test it is capped by the server
		var resp *workflowservice.ListActivityExecutionsResponse
		var err error
		resp, err = env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  maxPageSize + 1,
			Query:     fmt.Sprintf("ActivityType = '%s'", testActivityType),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetExecutions(), 1)

		// Get next page. Use pageSize > FrontendVisibilityMaxPageSize to test it is capped by the server
		resp, err = env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      maxPageSize + 1,
			Query:         fmt.Sprintf("ActivityType = '%s'", testActivityType),
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp.GetExecutions(), 1)

		// Ensure no more results
		resp, err = env.FrontendClient().ListActivityExecutions(s.Context(), &workflowservice.ListActivityExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      maxPageSize + 1,
			Query:         fmt.Sprintf("ActivityType = '%s'", testActivityType),
			NextPageToken: resp.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetExecutions())
		require.Nil(t, resp.GetNextPageToken())
	})
}

func (s *standaloneActivityTestSuite) TestCountActivityExecutions() {
	env := s.newTestEnv()
	t := s.T()

	activityID := env.Tv().ActivityID()
	activityType := env.Tv().ActivityType().GetName()
	env.startAndValidateActivity(s.Context(), t, activityID, env.Tv().TaskQueue().GetName())

	verifyCountQuery := func(t *testing.T, query string, expectedCount int) {
		t.Helper()
		s.AwaitTrue(
			func() bool {
				resp, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     query,
				})
				return err == nil && resp.GetCount() == int64(expectedCount)
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	t.Run("CountByActivityId", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ActivityId = '%s'", activityID), 1)
	})

	t.Run("CountByActivityType", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ActivityType = '%s'", activityType), 1)
	})

	t.Run("CountByExecutionStatus", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType), 1)
	})

	t.Run("CountByTaskQueue", func(t *testing.T) {
		verifyCountQuery(t, fmt.Sprintf("TaskQueue = '%s' AND ActivityType = '%s'", env.Tv().TaskQueue().GetName(), activityType), 1)
	})

	t.Run("GroupByExecutionStatus", func(t *testing.T) {
		groupByType := &commonpb.ActivityType{Name: "count-groupby-test-type"}
		taskQueue := env.Tv().TaskQueue().GetName()

		for i := range 3 {
			id := fmt.Sprintf("%s-%d", groupByType.Name, i)
			resp, err := env.startActivityWithType(s.Context(), id, taskQueue, groupByType)
			require.NoError(t, err)
			require.NotEmpty(t, resp.GetRunId())
		}

		query := fmt.Sprintf("ActivityType = '%s' GROUP BY ExecutionStatus", groupByType.Name)
		var resp *workflowservice.CountActivityExecutionsResponse
		s.AwaitTrue(
			func() bool {
				var err error
				resp, err = env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     query,
				})
				return err == nil && resp.GetCount() == 3
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)

		require.Len(t, resp.GetGroups(), 1)
		s.Equal(int64(3), resp.GetGroups()[0].GetCount())
		var groupValue string
		require.NoError(t, payload.Decode(resp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		s.Equal("Running", groupValue)
	})

	t.Run("CountByCustomSearchAttribute", func(t *testing.T) {
		customSAName := "ActivityCountCustomKeyword"
		customSAValue := "count-custom-sa-value"

		_, err := env.OperatorClient().AddSearchAttributes(s.Context(), &operatorservice.AddSearchAttributesRequest{
			Namespace: env.Namespace().String(),
			SearchAttributes: map[string]enumspb.IndexedValueType{
				customSAName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
		})
		require.NoError(t, err)

		s.AwaitTrue(func() bool {
			descResp, err := env.OperatorClient().ListSearchAttributes(s.Context(), &operatorservice.ListSearchAttributesRequest{
				Namespace: env.Namespace().String(),
			})
			if err != nil {
				return false
			}
			_, ok := descResp.CustomAttributes[customSAName]
			return ok
		}, 10*time.Second, 100*time.Millisecond)

		for i := range 2 {
			_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
				Namespace:           env.Namespace().String(),
				ActivityId:          fmt.Sprintf("count-custom-sa-%d", i),
				ActivityType:        &commonpb.ActivityType{Name: "count-custom-sa-type"},
				Identity:            env.Tv().WorkerIdentity(),
				Input:               defaultInput,
				TaskQueue:           &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().GetName()},
				StartToCloseTimeout: durationpb.New(1 * time.Minute),
				RequestId:           env.Tv().RequestID(),
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						customSAName: sadefs.MustEncodeValue(customSAValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
					},
				},
			})
			require.NoError(t, err)
		}

		s.AwaitTrue(
			func() bool {
				resp, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
					Namespace: env.Namespace().String(),
					Query:     fmt.Sprintf("%s = '%s'", customSAName, customSAValue),
				})
				return err == nil && resp.GetCount() == 2
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})

	t.Run("GroupByUnsupportedField", func(t *testing.T) {
		_, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "GROUP BY ActivityType",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.Contains(err.Error(), "'GROUP BY' clause is only supported for ExecutionStatus")
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("InvalidSearchAttribute", func(t *testing.T) {
		_, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})

	t.Run("NamespaceNotFound", func(t *testing.T) {
		_, err := env.FrontendClient().CountActivityExecutions(s.Context(), &workflowservice.CountActivityExecutionsRequest{
			Namespace: "non-existent-namespace",
			Query:     "",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
	})
}

func (s *standaloneActivityTestSuite) TestHeartbeat() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()
	heartbeatDetails := payloads.EncodeString("Heartbeat Details")

	t.Run("InvalidArgument", func(t *testing.T) {
		testCases := []struct {
			name        string
			taskToken   []byte
			expectedErr string
		}{
			{
				name:        "EmptyTaskToken",
				taskToken:   nil,
				expectedErr: "Task token not set on request",
			},
			{
				name:        "MalformedTaskToken",
				taskToken:   []byte("invalid-token-data"),
				expectedErr: "Error deserializing task token",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: env.Namespace().String(),
					TaskToken: tc.taskToken,
					Details:   heartbeatDetails,
				})
				require.Error(t, err)
				statusErr := serviceerror.ToStatus(err)
				require.NotNil(t, statusErr)
				require.Equal(t, codes.InvalidArgument, statusErr.Code())
				require.Contains(t, statusErr.Message(), tc.expectedErr)
			})
		}
	})

	t.Run("StaleToken", func(t *testing.T) {
		// Start an activity and get a valid task token, then complete it
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		_, err := env.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Heartbeat with stale token (activity already completed)
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("StaleAttemptToken", func(t *testing.T) {
		// Start an activity with retries, fail first attempt, then try to heartbeat with old token.
		// Use NextRetryDelay=1s to ensure the retry dispatch happens within test timeout.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and get task token for attempt 1
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		// Fail the task with NextRetryDelay to control retry timing
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to get attempt 2 (ensures retry has happened)
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Heartbeat with the attempt 2 token
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt2Resp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Try to heartbeat with the old attempt 1 token - should fail with NotFound
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.Error(t, err)
		statusErr := serviceerror.ToStatus(err)
		require.Equal(t, codes.NotFound, statusErr.Code())
		require.Contains(t, statusErr.Message(), fmt.Sprintf("activity not found for ID: %s", activityID))
	})

	t.Run("ByIDAfterRetry", func(t *testing.T) {
		// Regression test: by-ID heartbeat must succeed on attempt 2+.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                  defaultInput,
			ScheduleToCloseTimeout: durationpb.New(1 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt 1 retryably.
		attempt1Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempt1Resp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: attempt1Resp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable:   false,
					NextRetryDelay: durationpb.New(1 * time.Second),
				}},
			},
		})
		require.NoError(t, err)

		// Poll to start attempt 2.
		attempt2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, attempt2Resp.Attempt)

		// Heartbeat by ID — must succeed on attempt 2.
		_, err = env.FrontendClient().RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      attempt2Resp.ActivityRunId,
			ActivityId: activityID,
			Details:    heartbeatDetails,
		})
		require.NoError(t, err)
	})

	t.Run("MismatchedNamespaceToken", func(t *testing.T) {
		// Start an activity and get a valid task token, then complete it
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()
		_, err := env.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start an activity in a different namespace and try to heartbeat with existing token
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: externalNamespace,
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "Operation requested with a token from a different namespace.", invalidArgErr.Message)
	})

	// MismatchedTokenComponentRef tests that task tokens cannot be used across namespaces.
	// The validation ensures that the namespace in the request matches the namespace in the token's
	// ComponentRef, preventing cross-namespace token reuse attacks.
	t.Run("MismatchedTokenComponentRef", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		existingNamespace := env.Namespace().String()

		// Start activity in namespace A and get its task token
		_, err := env.startActivity(ctx, activityID, taskQueue)
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: existingNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Start another activity in namespace B with the same activity ID
		// (different namespaces allow same activity IDs)
		externalNamespace := env.ExternalNamespace().String()
		_, err = env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:    externalNamespace,
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
		})
		require.NoError(t, err)

		// Poll for the task from namespace B
		externalPollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: externalNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Deserialize both task tokens
		existingTask, err := tasktoken.NewSerializer().Deserialize(pollResp.TaskToken)
		require.NoError(t, err)

		externalTask, err := tasktoken.NewSerializer().Deserialize(externalPollResp.TaskToken)
		require.NoError(t, err)

		// Tamper with namespace A's token by replacing its ComponentRef with namespace B's ComponentRef
		existingTask.ComponentRef = externalTask.GetComponentRef()
		existingTaskToken, err := tasktoken.NewSerializer().Serialize(existingTask)
		require.NoError(t, err)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: existingNamespace,
			TaskToken: existingTaskToken,
			Details:   heartbeatDetails,
		})

		// Verify that the request is rejected with the correct error
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "token does not match namespace", invalidArgErr.Message)
	})

	t.Run("ResponseIncludesCancelRequested", func(t *testing.T) {
		// Start activity, worker accepts task, request cancellation, worker heartbeats.
		// Verify: heartbeat response has cancel_requested=true.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		})
		require.NoError(t, err)
		runID := startResp.RunId

		// Worker accepts task
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Heartbeat before cancellation - cancel_requested should be false
		hbResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)
		require.False(t, hbResp.CancelRequested)

		// Request cancellation
		_, err = env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			RequestId:  env.Tv().RequestID(),
			Reason:     "test cancellation",
		})
		require.NoError(t, err)

		// Heartbeat after cancellation - cancel_requested should be true
		hbResp, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)
		require.True(t, hbResp.CancelRequested)
	})

	t.Run("HeartbeatDetailsAvailableOnRetry", func(t *testing.T) {
		// Start activity (with retries), worker accepts, heartbeats with details,
		// worker fails the task. Worker accepts retry attempt.
		// Verify: retry task contains previous heartbeat details.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Millisecond),
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// First attempt: worker accepts task
		pollResp1, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.Attempt)
		require.Nil(t, pollResp1.HeartbeatDetails) // No heartbeat details on first attempt

		// Worker heartbeats with details
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Worker fails with retryable error
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: false,
					},
				},
			},
		})
		require.NoError(t, err)

		// Second attempt: worker accepts retry task
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)

		// Verify: heartbeat details from first attempt are available
		protorequire.ProtoEqual(t, heartbeatDetails, pollResp2.HeartbeatDetails)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)

		require.Equal(t, int64(2), desc.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count")
	})

	t.Run("ActivityTimesOutWithoutHeartbeat", func(t *testing.T) {
		// Start activity (no retries), worker accepts task, time passes beyond
		// heartbeat timeout, worker never heartbeats.
		// Verify: activity status is TIMED_OUT.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries
			},
		})
		require.NoError(t, err)

		// Worker accepts task (starts the activity)
		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Long poll for completion (heartbeat timeout will fire)
		pollResp, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT, pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
			"expected timeout type=Heartbeat but is %s", pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
	})

	t.Run("HeartbeatDetailsSurfacedOnTimeout", func(t *testing.T) {
		// Start activity (no retries), worker accepts task and records a heartbeat with details,
		// then stops heartbeating so the heartbeat timeout fires.
		// Verify: the timeout failure surfaces the last reported heartbeat details, matching the
		// workflow-embedded activity behavior.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries
			},
		})
		require.NoError(t, err)

		// Worker accepts task (starts the activity)
		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Worker records a heartbeat with details, then stops heartbeating.
		heartbeatDetails := payloads.EncodeString("progress before timeout")
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Long poll for completion (heartbeat timeout will fire)
		pollResp, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		timeoutFailureInfo := pollResp.GetOutcome().GetFailure().GetTimeoutFailureInfo()
		require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT, timeoutFailureInfo.GetTimeoutType(),
			"expected timeout type=Heartbeat but is %s", timeoutFailureInfo.GetTimeoutType())
		protorequire.ProtoEqual(t, heartbeatDetails, timeoutFailureInfo.GetLastHeartbeatDetails())

		// The details are also surfaced via DescribeActivityExecution.
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.RunId,
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, heartbeatDetails, desc.GetInfo().GetHeartbeatDetails())
	})

	t.Run("ActivityRetriesOnHeartbeatTimeout", func(t *testing.T) {
		// Start activity (with retries), worker accepts task, time passes beyond
		// heartbeat timeout, worker never heartbeats.
		// Verify: activity returns to SCHEDULED (or new task available).
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			HeartbeatTimeout:       durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Millisecond),
				MaximumAttempts: 2,
			},
		})
		require.NoError(t, err)

		// Attempt 1: worker accepts task
		pollTaskResp1, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollTaskResp1.Attempt)

		// Don't heartbeat - let it timeout and retry
		// Second attempt: worker accepts retry task
		pollTaskResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollTaskResp2.Attempt)
	})

	t.Run("HeartbeatKeepsActivityAlive", func(t *testing.T) {
		// Start activity, worker accepts, worker heartbeats within timeout,
		// more time passes, worker heartbeats again, worker completes.
		// Verify: activity status is COMPLETED.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries - timeout would be terminal
			},
		})
		require.NoError(t, err)

		// Worker accepts task
		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Heartbeat before timeout
		time.Sleep(600 * time.Millisecond) //nolint:forbidigo
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Wait again, then heartbeat again
		time.Sleep(600 * time.Millisecond) //nolint:forbidigo
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Complete the activity
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Verify activity completed successfully (didn't timeout)
		pollResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, pollResp.GetInfo().GetStatus(),
			"expected status=Completed but is %s", pollResp.GetInfo().GetStatus())
		require.Equal(t, int64(2), pollResp.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count")
		protorequire.ProtoEqual(t, defaultResult, pollResp.GetOutcome().GetResult())
	})

	t.Run("HeartbeatWithNoTimeoutDoesNotKillActivity", func(t *testing.T) {
		// Start activity with no heartbeat timeout, worker accepts, worker
		// heartbeats, wait for any spurious timeout task to fire, then
		// verify the activity is still running and can be completed.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			// No HeartbeatTimeout set.
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1,
			},
		})
		require.NoError(t, err)

		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Details:   heartbeatDetails,
		})
		require.NoError(t, err)

		// Wait long enough for a spurious zero-duration timeout task to fire.
		time.Sleep(2 * time.Second) //nolint:forbidigo

		// Activity should still be running.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus(),
			"activity should still be running but is %s", descResp.GetInfo().GetStatus())
		require.Equal(t, int64(1), descResp.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count")

		// Complete the activity to confirm it's still operable.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus(),
			"expected status=Completed but is %s", descResp.GetInfo().GetStatus())
		require.Equal(t, int64(1), descResp.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count")
		protorequire.ProtoEqual(t, defaultResult, descResp.GetOutcome().GetResult())
	})

	t.Run("RecordHeartbeatByIDStaysAlive", func(t *testing.T) {
		// Start activity, worker accepts, worker heartbeats within timeout,
		// more time passes, worker heartbeats again, worker completes.
		// Verify: activity status is COMPLETED.
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			HeartbeatTimeout:    durationpb.New(1 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // No retries - timeout would be terminal
			},
		})
		require.NoError(t, err)

		// Worker accepts task
		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollTaskResp.TaskToken)

		// Heartbeat before timeout
		_, err = env.FrontendClient().RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Details:    heartbeatDetails,
		})
		require.NoError(t, err)

		// Complete the activity
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollTaskResp.TaskToken,
			Result:    defaultResult,
		})
		require.NoError(t, err)

		// Verify activity completed successfully (didn't timeout)
		pollResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, pollResp.GetInfo().GetStatus(),
			"expected status=Completed but is %s", pollResp.GetInfo().GetStatus())
		require.Equal(t, int64(1), pollResp.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count")
		protorequire.ProtoEqual(t, defaultResult, pollResp.GetOutcome().GetResult())
	})

	t.Run("HeartbeatCountIncrementsPerHeartbeat", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
		})
		require.NoError(t, err)

		pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		const numHeartbeats = 5
		for i := range numHeartbeats {
			_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
				Namespace: env.Namespace().String(),
				TaskToken: pollTaskResp.TaskToken,
				Details:   heartbeatDetails,
			})
			require.NoError(t, err)

			descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			require.Equal(t, int64(i+1), descResp.GetInfo().GetTotalHeartbeatCount(), "total heartbeat count after heartbeat %d", i+1)
		}
	})
}

func (s *standaloneActivityTestSuite) TestStartDelay() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("Dispatch", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		// 3s > timerSafetyMargin so the require.Never below has a non-trivial window
		// (startDelay - timerSafetyMargin = 1.5s) to assert the activity stays SCHEDULED.
		startDelay := 3 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           requestID,
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)
		require.NotEmpty(t, startResp.GetRunId())
		require.True(t, startResp.Started)

		// Verify activity stays in SCHEDULED state throughout the delay (not dispatched early).
		require.Never(t, func() bool {
			describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			return err != nil || describeResp.GetInfo().GetRunState() != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
		}, startDelay-timerSafetyMargin, 100*time.Millisecond,
			"expected activity to remain in SCHEDULED state during start delay")

		// Poll blocks until the task is dispatched after the delay elapses.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken(), "expected task after start delay")

		// CurrentAttemptScheduledTime should be exactly ScheduledTime + StartDelay.
		// Both values are computed server-side from the same fields, so no tolerance needed.
		expectedDispatchTime := pollResp.GetScheduledTime().AsTime().Add(startDelay)
		require.Equal(t, expectedDispatchTime, pollResp.GetCurrentAttemptScheduledTime().AsTime())
	})

	t.Run("CompleteAfterDelay", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		// Short so the test waits ~1s for dispatch; the value isn't load-bearing for this test.
		startDelay := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           requestID,
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Poll blocks until the task is dispatched after the delay elapses.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		protorequire.ProtoEqual(t, defaultResult, descResp.GetOutcome().GetResult())
	})

	t.Run("ScheduleToStartTimeoutExtended", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		// require.Never window = startDelay + scheduleToStart - timerSafetyMargin = 2.5s. Total wait is
		// startDelay + scheduleToStart = 4s; 3s+1s keeps the test fast while leaving a comfortable margin.
		startDelay := 3 * time.Second
		scheduleToStartTimeout := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Second),
			ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
			StartDelay:             durationpb.New(startDelay),
			RequestId:              requestID,
		})
		require.NoError(t, err)

		// Activity should stay running (not timed out) throughout startDelay + most of scheduleToStartTimeout.
		// The timeout should only fire after startDelay + scheduleToStartTimeout.
		require.Never(t, func() bool {
			descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || descResp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, startDelay+scheduleToStartTimeout-timerSafetyMargin, 100*time.Millisecond,
			"activity should not time out before startDelay + scheduleToStartTimeout")

		// Now wait for the ScheduleToStart timeout to actually fire (measured from after delay).
		await.Require(s.Context(), t, func(c *await.T) {
			resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("ScheduleToCloseTimeoutExtended", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		// Same shape as ScheduleToStartTimeoutExtended; require.Never window = 2.5s, total wait ~4s.
		startDelay := 3 * time.Second
		scheduleToCloseTimeout := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(startDelay),
			RequestId:              requestID,
		})
		require.NoError(t, err)

		// ExpirationTime should account for start delay: ScheduleTime + StartDelay + ScheduleToCloseTimeout.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		expectedExpiration := descResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay).Add(scheduleToCloseTimeout)
		require.Equal(t, expectedExpiration, descResp.GetInfo().GetExpirationTime().AsTime())

		// Activity should stay running (not timed out) throughout startDelay + most of scheduleToCloseTimeout.
		// The timeout should only fire after startDelay + scheduleToCloseTimeout.
		require.Never(t, func() bool {
			descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || descResp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, startDelay+scheduleToCloseTimeout-timerSafetyMargin, 100*time.Millisecond,
			"activity should not time out before startDelay + scheduleToCloseTimeout")

		// Now wait for the ScheduleToClose timeout to actually fire (measured from after delay).
		await.Require(s.Context(), t, func(c *await.T) {
			resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("CancelDuringDelay", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		startDelay := 10 * time.Second // Long delay so the activity stays in SCHEDULED state.

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           requestID,
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Cancel while still in the delay period (SCHEDULED state). Should immediately cancel.
		_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			RequestId:  testcore.RandomizeStr("cancel-request-id"),
			Reason:     "cancel during delay",
		})
		require.NoError(t, err)

		// Activity should be CANCELED immediately, not waiting for the delay to elapse.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, descResp.GetInfo().GetStatus())
	})

	t.Run("TerminateDuringDelay", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		startDelay := 10 * time.Second // Long delay so the activity stays in SCHEDULED state.

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           requestID,
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Terminate while still in the delay period (SCHEDULED state).
		_, err = env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "terminate during delay",
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	// Validates that the retry deadline accounts for start delay: the effective ScheduleToClose
	// window is ScheduleTime + StartDelay + ScheduleToClose. ScheduleToClose is set short enough
	// that without the start delay extension, the retry after attempt 1 fails would be rejected
	// (fail at ~T+2s, retryInterval 1s, deadline without extension = T+3s). With the extension
	// the deadline is T+5s, so the server allows the retry and we observe attempt 2.
	t.Run("RetryWithStartDelay", func(t *testing.T) {

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := testcore.RandomizeStr("request-id")
		// scheduleToClose < startDelay would reject the retry if the deadline didn't extend by
		// startDelay. 2s+3s makes the bug case (deadline = T+3s) reject the retry, while the
		// correct extended deadline (T+5s) allows attempt 2.
		startDelay := 2 * time.Second
		scheduleToCloseTimeout := 3 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			RequestId:              requestID,
			StartDelay:             durationpb.New(startDelay),
		})
		require.NoError(t, err)
		require.NotEmpty(t, startResp.GetRunId())

		// Poll blocks until the task is dispatched after the delay elapses.
		pollResp1, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp1.GetTaskToken())
		require.EqualValues(t, 1, pollResp1.GetAttempt())

		// Fail attempt 1 with a retryable error and short retry delay.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: false,
					},
				},
			},
		})
		require.NoError(t, err)
		failTime := time.Now()

		// Attempt 2 should be scheduled (start delay extended the ScheduleToClose deadline).
		// It should arrive within the retry backoff interval, NOT re-applying the start delay.
		pollResp2, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp2.GetTaskToken(), "expected retry attempt 2 to be dispatched")
		require.EqualValues(t, 2, pollResp2.GetAttempt())
		require.Less(t, time.Since(failTime), startDelay,
			"retry was dispatched after startDelay, suggesting start delay was re-applied")

		// Complete attempt 2.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		require.EqualValues(t, 2, descResp.GetInfo().GetAttempt())
		protorequire.ProtoEqual(t, defaultResult, descResp.GetOutcome().GetResult())
	})

	// UpdateActivityOptions can mutate start_delay while the activity is inside its delay
	// window. The new value is anchored to the original schedule_time; if the resulting
	// first-dispatch time is in the past, the activity dispatches immediately. Setting
	// start_delay to 0 is the "run-now" path.
	s.Run("UpdateToZero_DispatchImmediately", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// 60s is much larger than the 10s poll context below: if start_delay weren't actually updated
		// to 0, the poll would time out, failing the test.
		startDelay := 60 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Update start_delay to 0 while we're still in the long delay window.
		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, updateResp.GetActivityOptions().GetStartDelay().AsDuration())

		// Poll should succeed quickly (well before the original 60s delay would have elapsed).
		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken(), "expected immediate dispatch after start_delay=0")
	})

	s.Run("UpdateLonger_ExtendsDispatch", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// require.Never below asserts SCHEDULED past originalDelay+timerSafetyMargin = 2.5s, which only
		// holds if the extended delay is in effect; newDelay = 3s > 2.5s so the assertion is meaningful.
		originalDelay := 1 * time.Second
		newDelay := 3 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(newDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Activity should stay SCHEDULED past the original 2s delay, since the new target is at T+5s.
		require.Never(t, func() bool {
			descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || descResp.GetInfo().GetRunState() != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
		}, originalDelay+timerSafetyMargin, 100*time.Millisecond,
			"activity should remain SCHEDULED past the original delay since start_delay was extended")

		// Polling should eventually succeed once the new delay elapses.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())
		expectedDispatchTime := pollResp.GetScheduledTime().AsTime().Add(newDelay)
		require.Equal(t, expectedDispatchTime, pollResp.GetCurrentAttemptScheduledTime().AsTime())
	})

	s.Run("UpdateShorter_ReducesDispatch", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// 30s is well past the 5s poll context: if the update didn't shorten start_delay, the poll
		// would time out. 2s is short enough that the poll returns quickly under the new delay.
		originalDelay := 30 * time.Second
		newDelay := 2 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(newDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Poll should succeed within the new (shorter) delay window, well before the original.
		pollCtx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())
		expectedDispatchTime := pollResp.GetScheduledTime().AsTime().Add(newDelay)
		require.Equal(t, expectedDispatchTime, pollResp.GetCurrentAttemptScheduledTime().AsTime())
	})

	s.Run("UpdatePastTime_DispatchesImmediately", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// 60s is much larger than the 10s poll context: if start_delay weren't reduced, the poll fails.
		originalDelay := 60 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// Give a small wall-clock advance so "scheduleTime + 1ms" is already in the past.
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(1 * time.Millisecond)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken(),
			"expected immediate dispatch when scheduleTime+newStartDelay is in the past")
	})

	s.Run("UpdateAfterDispatch_Rejected", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		})
		require.NoError(t, err)

		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(10 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.Error(t, err)
		var failedPrecond *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecond)
		require.Contains(t, failedPrecond.Message, "start_delay")
	})

	s.Run("UpdateNegative_Rejected", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(60 * time.Second),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(-1 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.Error(t, err)
		var invArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invArg)
		require.Contains(t, invArg.Message, "invalid StartDelay")
	})

	// An unrelated options update during the delay window must not shrink the ScheduleToClose deadline by start_delay.
	s.Run("UpdateUnrelatedField_ScheduleToCloseRespectsDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Effective deadline = scheduleTime + startDelay + scheduleToClose = T+4s. require.Never window
		// = 4-timerSafetyMargin = 2.5s. A bug that anchored the deadline to scheduleTime alone would
		// time out at T+1s, well inside the window.
		startDelay := 3 * time.Second
		scheduleToCloseTimeout := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Update an unrelated field (heartbeat timeout) without touching start_delay.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(5 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		// ExpirationTime must still reflect scheduleTime + startDelay + scheduleToClose.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		expectedExpiration := descResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay).Add(scheduleToCloseTimeout)
		require.Equal(t, expectedExpiration, descResp.GetInfo().GetExpirationTime().AsTime())

		// Must not time out before startDelay + scheduleToClose elapses (would happen with the bug).
		require.Never(t, func() bool {
			resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || resp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, startDelay+scheduleToCloseTimeout-timerSafetyMargin, 100*time.Millisecond,
			"activity timed out earlier than scheduleTime + startDelay + scheduleToClose")

		// And eventually does time out at the correct deadline.
		await.Require(s.Context(), t, func(c *await.T) {
			resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
		}, 10*time.Second, 100*time.Millisecond)
	})

	s.Run("UpdateRestoreOriginal_RestoresStartDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Long enough that the activity never actually dispatches during the test; we only assert that
		// the restored start_delay value matches the original via Describe.
		originalDelay := 30 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// First, update start_delay to a shorter value.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(1 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, 1*time.Second, descResp.GetInfo().GetStartDelay().AsDuration())

		// Now restore original.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			RestoreOriginal: true,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, originalDelay, descResp.GetInfo().GetStartDelay().AsDuration())
	})

	// RestoreOriginal on an already-dispatched activity must NOT touch start_delay. Restoring it would
	// shift the close-deadline recomputation (firstDispatchTime + scheduleToClose) and rewrite the
	// recorded execution_time without any functional effect on the running attempt.
	s.Run("UpdateRestoreOriginal_OnStartedActivity_LeavesStartDelayUnchanged", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// originalDelay just needs to be non-zero so we can detect a buggy restore. scheduleToClose
		// is large enough that the activity doesn't time out during the test.
		originalDelay := 5 * time.Second
		scheduleToCloseTimeout := 30 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// While still in delay window, set start_delay=0 so the activity dispatches immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Poll: activity dispatches and worker picks up, so the status moves to STARTED.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, descResp.GetInfo().GetRunState())
		scheduleTime := descResp.GetInfo().GetScheduleTime().AsTime()
		expectedDeadlineAfterUpdate := scheduleTime.Add(scheduleToCloseTimeout)
		require.Equal(t, expectedDeadlineAfterUpdate, descResp.GetInfo().GetExpirationTime().AsTime(),
			"close-deadline should be scheduleTime + scheduleToClose after start_delay was set to 0")

		// RestoreOriginal while STARTED.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			RestoreOriginal: true,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, descResp.GetInfo().GetStartDelay().AsDuration(),
			"start_delay should not be restored when the activity has already dispatched")
		require.Equal(t, expectedDeadlineAfterUpdate, descResp.GetInfo().GetExpirationTime().AsTime(),
			"close-deadline should not shift when start_delay is left untouched")
	})

	// Same invariant as the STARTED case, but exercises the retry-backoff path: after the first
	// attempt has dispatched and failed, the activity is back in SCHEDULED. RestoreOriginal must
	// still not rewrite start_delay, since the first dispatch already happened.
	s.Run("UpdateRestoreOriginal_OnRetryBackoff_LeavesStartDelayUnchanged", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalDelay := 5 * time.Second
		scheduleToCloseTimeout := 5 * time.Minute

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(originalDelay),
			RetryPolicy: &commonpb.RetryPolicy{
				// Retry interval long enough to keep the activity in backoff through RestoreOriginal,
				// short enough to fit comfortably inside ScheduleToCloseTimeout so the activity is
				// not pre-emptively timed out instead of scheduled for retry.
				InitialInterval: durationpb.New(1 * time.Minute),
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Drop start_delay so the activity dispatches immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		scheduleTime := descResp.GetInfo().GetScheduleTime().AsTime()
		expectedDeadlineAfterUpdate := scheduleTime.Add(scheduleToCloseTimeout)

		// Fail the attempt with a retryable failure to drive the activity back to SCHEDULED.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp.GetInfo().GetRunState(),
			"activity should be in retry backoff (SCHEDULED) after a retryable failure")

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			RestoreOriginal: true,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, descResp.GetInfo().GetStartDelay().AsDuration(),
			"start_delay should not be restored when the activity has already dispatched")
		require.Equal(t, expectedDeadlineAfterUpdate, descResp.GetInfo().GetExpirationTime().AsTime(),
			"close-deadline should not shift when start_delay is left untouched")
	})

	s.Run("ResetRestoreOriginal_RestoresStartDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// originalDelay short so the poll below succeeds quickly after reset. updatedDelay must be
		// much larger than the 10s poll context: if the reset didn't restore start_delay, the poll
		// would time out under the larger updated delay.
		originalDelay := 1 * time.Second
		updatedDelay := 30 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// Mutate start_delay via update to a much longer value so the activity would not naturally dispatch before reset.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(updatedDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, updatedDelay, descResp.GetInfo().GetStartDelay().AsDuration())

		// Reset with RestoreOriginalOptions=true so start_delay is restored alongside the other options.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, originalDelay, descResp.GetInfo().GetStartDelay().AsDuration())

		// Activity should be pollable after reset. The original delay was 1s so the poll succeeds well within
		// the 10s timeout (and well before updatedDelay would have allowed dispatch).
		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())
		require.EqualValues(t, 1, pollResp.GetAttempt())
	})

	// Reset+RestoreOriginalOptions restores start_delay AND clamps the next dispatch to the original
	// scheduleTime + start_delay. Without the clamp, zeroing start_delay then resetting would dispatch
	// immediately, defeating the "undo my mistake" intent.
	s.Run("ResetRestoreOriginal_PreservesOriginalWallClockTarget", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// 3s > timerSafetyMargin so a buggy "dispatch at reset time" produces lastStart ≈ T0+ε,
		// while executionTime = T0+3s. The assertion (lastStart + 1.5s ≥ T0+3s) fails by ~1.5s.
		originalDelay := 3 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// Zero out start_delay so the activity would dispatch immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Reset+RestoreOriginalOptions: restores start_delay AND should clamp dispatch to original target.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Worker eventually picks up.
		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		// lastStart (first worker pickup) should be at-or-after the original target (execution_time =
		// scheduleTime + originalDelay), within a small safety margin to absorb timer firing imprecision.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		executionTime := descResp.GetInfo().GetExecutionTime().AsTime()
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, executionTime.IsZero(), "execution_time should be populated")
		require.False(t, lastStart.IsZero(), "last_started_time should be populated after pickup")
		require.GreaterOrEqual(t, lastStart.Add(timerSafetyMargin).UnixNano(), executionTime.UnixNano(),
			"activity dispatched before its original execution_time; RestoreOriginalOptions did not honor the original delay")
	})

	// If the original delay has already elapsed when Reset+RestoreOriginalOptions is called, the clamp
	// doesn't apply and dispatch fires at "now" (the reset time).
	s.Run("ResetRestoreOriginal_AfterDelayElapsed_DispatchesImmediately", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Short so the Sleep below (2s) doesn't make the test slow; only needs to elapse before reset.
		originalDelay := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// Wait past the original delay so firstDispatchTime is in the past when we reset.
		time.Sleep(2 * time.Second) //nolint:forbidigo

		resetTime := time.Now()
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		// lastStart should be close to the reset time since the delay was already in the past.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStart.IsZero(), "last_started_time should be populated after pickup")
		require.WithinDuration(t, resetTime, lastStart, timerSafetyMargin,
			"dispatch did not fire promptly after reset; expected close to resetTime since delay was already in the past")
	})

	// Reset+RestoreOriginalOptions on a STARTED activity must NOT touch start_delay. The first dispatch
	// has already happened so altering the value has no actual effect on dispatch timing.
	s.Run("ResetRestoreOriginal_OnStartedActivity_LeavesStartDelayUnchanged", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Just needs to be non-zero so we can detect a buggy restore; value isn't load-bearing.
		originalDelay := 5 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Second),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// While still in delay window, set start_delay=0 so the activity dispatches immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Poll: activity dispatches and worker picks up, so the status moves to STARTED.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, descResp.GetInfo().GetRunState())
		require.EqualValues(t, 0, descResp.GetInfo().GetStartDelay().AsDuration(), "start_delay should be 0 after the update")

		// Reset+RestoreOriginalOptions while STARTED.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// start_delay should remain at the post-update value (0).
		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, descResp.GetInfo().GetStartDelay().AsDuration(),
			"start_delay should not be restored when the activity has already dispatched")
	})

	// Same invariant as the STARTED case, but exercises the retry-backoff path: after the first
	// attempt dispatched and failed, the activity is back in SCHEDULED. Reset+RestoreOriginalOptions
	// must not rewrite start_delay since the first dispatch already happened.
	s.Run("ResetRestoreOriginal_OnRetryBackoff_LeavesStartDelayUnchanged", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalDelay := 5 * time.Second
		scheduleToCloseTimeout := 5 * time.Minute

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(originalDelay),
			RetryPolicy: &commonpb.RetryPolicy{
				// Retry interval long enough to keep the activity in backoff through the Reset call,
				// short enough to fit comfortably inside ScheduleToCloseTimeout so the activity is
				// not pre-emptively timed out instead of scheduled for retry.
				InitialInterval: durationpb.New(1 * time.Minute),
				MaximumAttempts: 3,
			},
		})
		require.NoError(t, err)

		// Drop start_delay so the activity dispatches immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		// Fail the attempt with a retryable failure to drive the activity back to SCHEDULED.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp.GetInfo().GetRunState(),
			"activity should be in retry backoff (SCHEDULED) after a retryable failure")

		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.EqualValues(t, 0, descResp.GetInfo().GetStartDelay().AsDuration(),
			"start_delay should not be restored when the activity has already dispatched")
	})

	s.Run("ResetRestoreOriginal_RecomputesScheduleToStartAndScheduleToClose", func(s *standaloneActivityTestSuite) {
		// Create an activity with a long delay and a short ScheduleToClose.
		// Update it to get rid of the delay -> pulls the ScheduleToClose in to a short deadline.
		// Reset the delay back to the long value -> pushes the ScheduleToClose deadline out to delay + ScheduleToClose
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalStartDelay := 4 * time.Second
		scheduleToCloseTimeout := 2 * time.Second

		// Create it with ScheduleToClose deadline = 2s + 4s
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
			StartDelay:             durationpb.New(originalStartDelay),
		})
		require.NoError(t, err)
		describe := func() *workflowservice.DescribeActivityExecutionResponse {
			resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(t, err)
			return resp
		}
		desc := describe()
		require.Equal(t,
			desc.Info.ScheduleTime.AsTime().Add(originalStartDelay+scheduleToCloseTimeout),
			desc.Info.ExpirationTime.AsTime())

		// Update start_delay to 0: recreates the ScheduleToClose deadline = 0s + 2s.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		desc = describe()
		require.Equal(t,
			desc.Info.ScheduleTime.AsTime().Add(0+scheduleToCloseTimeout),
			desc.Info.ExpirationTime.AsTime())

		// Reset start_delay to 4s: recreates ScheduleToClose deadline = 2s + 4s
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)
		desc = describe()
		require.Equal(t,
			desc.Info.ScheduleTime.AsTime().Add(originalStartDelay+scheduleToCloseTimeout),
			desc.Info.ExpirationTime.AsTime())

		require.Never(t, func() bool {
			return describe().GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, 3500*time.Millisecond, 200*time.Millisecond,
			"ScheduleToStart and ScheduleToClose timeouts should have been pushed back to 6s by the start delay")

	})

	// The guard accepts the field mask path in either snake_case or camelCase form.
	s.Run("UpdateCamelCaseFieldMask_Rejected", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(60 * time.Second),
		})
		require.NoError(t, err)

		// CamelCase path should still trigger the start_delay guard.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(-1 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"startDelay"}},
		})
		require.Error(t, err)
		var invArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invArg)
		require.Contains(t, invArg.Message, "StartDelay")
	})

	// Verifies the ScheduleToStart timeout is reissued anchored to the updated firstDispatchTime. Without
	// correct re-anchoring, the activity would time out at scheduleTime + originalDelay + scheduleToStartTimeout
	// instead of at the extended deadline.
	s.Run("UpdateStartDelay_ScheduleToStartReanchored", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// originalDelay short so the update happens while in delay window. updatedDelay+scheduleToStart
		// must comfortably exceed (originalDelay+scheduleToStart+timerSafetyMargin) so a buggy "anchored
		// to originalDelay" would time out at T+2s, well before the correct T+5s deadline.
		originalDelay := 1 * time.Second
		updatedDelay := 4 * time.Second
		scheduleToStartTimeout := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Second),
			ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
			StartDelay:             durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(updatedDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		// Must not time out before scheduleTime + updatedDelay + scheduleToStartTimeout = T+5s.
		// With the bug, would time out at scheduleTime + originalDelay + scheduleToStartTimeout = T+2s.
		require.Never(t, func() bool {
			descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || descResp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, updatedDelay+scheduleToStartTimeout-timerSafetyMargin, 100*time.Millisecond,
			"activity should not time out before scheduleTime + updatedDelay + scheduleToStartTimeout")

		// Eventually times out at the correct deadline.
		await.Require(s.Context(), t, func(c *await.T) {
			resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
		}, 10*time.Second, 100*time.Millisecond)
	})

	// During retry backoff an unrelated options update must not re-apply start_delay to the retry timing.
	s.Run("UpdateDuringRetryBackoff_DoesNotReapplyStartDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Equal so the upper bound (retryInterval+startDelay = 4s) is meaningful: a bug that re-applied
		// start_delay would push retry to ~4s after fail, missing the upper bound.
		startDelay := 2 * time.Second
		retryInterval := 2 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(60 * time.Second),
			ScheduleToCloseTimeout: durationpb.New(60 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(retryInterval),
				BackoffCoefficient: 1.0,
				MaximumAttempts:    3,
			},
			StartDelay: durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Wait for and fail attempt 1.
		pollResp1, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.GetAttempt())

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)
		failTime := time.Now()

		// Update an unrelated option (heartbeat) while the activity is in retry backoff.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(10 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		// Attempt 2 should arrive at the retry interval (~retryInterval after fail), not delayed by start_delay.
		pollResp2, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp2.GetTaskToken())
		require.EqualValues(t, 2, pollResp2.GetAttempt())
		require.Less(t, time.Since(failTime), retryInterval+startDelay,
			"retry was delayed beyond the retry interval, suggesting start_delay was incorrectly re-applied")
	})

	// Pause during the delay window must not let unpause dispatch before the original wall-clock
	// target. Without respectStartDelay in unpause(), pausing then quickly unpausing would let
	// the activity dispatch at "now," skipping the rest of the original delay.
	s.Run("PauseDuringDelay_UnpauseHonorsWallClockTarget", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// The assertion below allows lastStart to land within timerSafetyMargin (1.5s) of the original
		// target (T0+3s). 3s leaves room for a regression that dispatches immediately on unpause to fall
		// well below T0+1.5s and fail visibly.
		startDelay := 3 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		expectedExecution := descResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay)

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStart.IsZero())
		require.GreaterOrEqual(t, lastStart.Add(timerSafetyMargin).UnixNano(), expectedExecution.UnixNano(),
			"activity dispatched before its original execution_time; unpause did not honor the original delay")
	})

	// Reset (without RestoreOriginalOptions) of an activity still inside its start_delay window must
	// honor the remaining delay, as Unpause does.
	s.Run("ResetDuringDelay_HonorsRemainingStartDelay", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		startDelay := 5 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		expectedDispatchTime := descResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay)

		// Reset while still in the delay window, before any worker pickup.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)

		// Poll: should not transition to STARTED until the start delay has elapsed
		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		// Assert that it STARTED after the expected dispatch time.
		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStart.IsZero())
		require.GreaterOrEqual(t, lastStart.Add(timerSafetyMargin).UnixNano(), expectedDispatchTime.UnixNano(),
			"activity dispatched before start delay end; Reset did not honor the remaining start delay")
	})

	s.Run("ResetAfterFirstStart_ReportsResetDispatchTime", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		startDelay := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RetryPolicy:         defaultRetryPolicy,
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		pollResp1, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.GetAttempt())
		firstAttemptDispatchTime := pollResp1.GetCurrentAttemptScheduledTime().AsTime()

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable:   false,
						NextRetryDelay: durationpb.New(time.Hour),
					},
				},
			},
			Identity: defaultIdentity,
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp.GetInfo().GetRunState())

		// After the first worker pickup, reset rewinds the attempt count to 1 but must report
		// the reset attempt's dispatch time, not the original first dispatch time.
		resetTime := time.Now()
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp2, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		pollTime := time.Now()
		require.EqualValues(t, 1, pollResp2.GetAttempt())
		resetAttemptDispatchTime := pollResp2.GetCurrentAttemptScheduledTime().AsTime()
		require.NotEqual(t, firstAttemptDispatchTime, resetAttemptDispatchTime)
		require.GreaterOrEqual(t, resetAttemptDispatchTime.Add(timerSafetyMargin).UnixNano(), resetTime.UnixNano())
		require.LessOrEqual(t, resetAttemptDispatchTime.UnixNano(), pollTime.Add(timerSafetyMargin).UnixNano())
	})

	// If the delay window has already elapsed by the time the activity is unpaused, the activity
	// should dispatch immediately (no leftover delay to honor).
	s.Run("PauseDuringDelay_UnpauseAfterDelay_DispatchesImmediately", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Short so the sleep below doesn't make the test slow; only needs to be > 0.
		startDelay := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// 2s > startDelay (1s) to ensure firstDispatchTime is firmly in the past at unpause time,
		// even accounting for clock variance.
		time.Sleep(2 * time.Second) //nolint:forbidigo

		unpauseTime := time.Now()
		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStart.IsZero())
		require.WithinDuration(t, unpauseTime, lastStart, timerSafetyMargin,
			"expected near-immediate dispatch on unpause when the original delay had already elapsed")
	})

	// Pause/unpause after the first dispatch must NOT re-apply start_delay to the next attempt.
	// Verifies that respectStartDelay in unpause() correctly no-ops on the post-dispatch path
	// via FirstAttemptStartedTime.
	s.Run("PausePostDispatch_NoStartDelayReapplied", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Must exceed timerSafetyMargin so a re-applied delay would visibly miss the margin below.
		startDelay := 2 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				// Short so the retry would normally fire right away post-yield; the pause is what keeps it
				// from dispatching. Keeps the test fast without zeroing the interval.
				InitialInterval:    durationpb.New(100 * time.Millisecond),
				BackoffCoefficient: 1.0,
				MaximumAttempts:    3,
			},
			StartDelay: durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Wait for first dispatch and have the worker pick up attempt 1.
		pollResp1, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.GetAttempt())

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Worker yields (attempt 1 fails). Activity transitions PAUSE_REQUESTED -> PAUSED.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		unpauseTime := time.Now()
		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		// Attempt 2 should dispatch close to unpause time, NOT after an additional start_delay duration.
		pollResp2, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.GetAttempt())

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStarted := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStarted.IsZero())
		require.WithinDuration(t, unpauseTime, lastStarted, timerSafetyMargin,
			"attempt 2 was delayed beyond unpause time; start_delay should not re-apply post-dispatch")
	})

	// ScheduleToStart counts from when the activity is dispatched, so on unpause it must be re-anchored
	// to scheduleTime + start_delay + scheduleToStart, not to unpauseTime + scheduleToStart (which
	// would fire prematurely while the activity is still inside its delay window).
	s.Run("PauseDuringDelay_ScheduleToStartReanchored", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Window for the require.Never below is startDelay+scheduleToStart-timerSafetyMargin = 2.5s.
		// A buggy "anchored to unpauseTime" would fire ScheduleToStart at ~unpauseTime+1s ≈ T0+1.5s,
		// well inside the 2.5s window even on slow CI.
		startDelay := 3 * time.Second
		scheduleToStartTimeout := 1 * time.Second // Small but non-zero so the deadline is meaningful.

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Second),
			ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
			StartDelay:             durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Pause + unpause quickly within the delay window.
		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		// No worker polls. ScheduleToStart should fire at scheduleTime + start_delay + scheduleToStart,
		// not at unpauseTime + scheduleToStart. Verify no timeout before the correct deadline.
		require.Never(t, func() bool {
			descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			return err != nil || descResp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, startDelay+scheduleToStartTimeout-timerSafetyMargin, 100*time.Millisecond,
			"activity timed out before scheduleTime + start_delay + scheduleToStart; ScheduleToStart was not re-anchored")

		// Eventually times out at the correct deadline.
		await.Require(s.Context(), t, func(c *await.T) {
			resp, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, resp.GetInfo().GetStatus())
		}, 10*time.Second, 100*time.Millisecond)
	})

	// Multiple pause/unpause cycles in the delay window must not drift the wall-clock target. The
	// dispatch should still fire at scheduleTime + start_delay regardless of how many cycles
	// happened, since the target is anchored to schedule_time (not to the most recent unpause).
	s.Run("PauseDuringDelay_MultipleCyclesHonorWallClockTarget", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// Lower-bound margin below is startDelay-timerSafetyMargin = 3.5s. A buggy "dispatch at last
		// unpauseTime" would produce lastStart at most a few seconds in (cycle latency), well below
		// T0+3.5s. 5s keeps the catch reliable even if pause+unpause cycles take ~3s on slow CI.
		startDelay := 5 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		expectedExecution := descResp.GetInfo().GetScheduleTime().AsTime().Add(startDelay)

		for range 2 {
			_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
				Identity:   defaultIdentity,
				Reason:     "test-pause",
			})
			require.NoError(t, err)
			_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
				Identity:   defaultIdentity,
			})
			require.NoError(t, err)
		}

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		lastStart := descResp.GetInfo().GetLastStartedTime().AsTime()
		require.False(t, lastStart.IsZero())
		require.GreaterOrEqual(t, lastStart.Add(timerSafetyMargin).UnixNano(), expectedExecution.UnixNano(),
			"activity dispatched before its original execution_time; multiple pause cycles let the target drift")
	})

	// start_delay is mutable while the first dispatch is still pending, which includes while the
	// activity is paused in the delay window: UpdateActivityOptions can change anything while
	// paused, including the start delay.
	s.Run("UpdateWhilePaused_ShortensDispatch", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		// If the paused update didn't take effect, the poll would time out.
		originalDelay := 60 * time.Second
		// The poll returns quickly under the updated delay.
		newDelay := 2 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		// Pause while inside the delay window -> PAUSED
		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED, descResp.GetInfo().GetStatus())

		// Update start_delay while paused
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(newDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, newDelay, descResp.GetInfo().GetStartDelay().AsDuration())

		// Unpause; dispatch honors the updated (shorter) delay, so the poll succeeds well within the 10s
		// context and long before the original 60s delay would have elapsed.
		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		pollCtx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken(), "expected dispatch under the updated start_delay after unpause")
	})

	// While paused before the first attempt, start_delay stays editable even after the original
	// delay window has elapsed: extending start_delay defers the dispatch that unpause would
	// otherwise perform immediately.
	s.Run("UpdateWhilePaused_AfterWindow_ExtendsDispatch", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalDelay := 3 * time.Second
		newDelay := 30 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())

		// wait until original delay window is in the past.
		time.Sleep(originalDelay) //nolint:forbidigo

		// The first attempt never started, so start_delay is still editable despite the elapsed window.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(newDelay)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)
		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, newDelay, descResp.GetInfo().GetStartDelay().AsDuration())

		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		// Dispatch now honors the extended delay (~30s out), so a short poll must find no task; had the
		// update not taken effect, the elapsed original delay would dispatch immediately on unpause.
		pollCtx, cancel := context.WithTimeout(s.Context(), 3*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		// A long-poll that gets no task within its timeout gets a non-error empty response
		require.NoError(t, err)
		require.Empty(t, pollResp.GetTaskToken(),
			"activity must not dispatch within the extended start_delay after unpause")
	})

	// Symmetric to ExtendsDispatch: while paused before the first attempt, start_delay can also be
	// retracted after the original window has elapsed, so unpause dispatches immediately.
	s.Run("UpdateWhilePaused_AfterWindow_RetractsDispatch", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalDelay := 10 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(originalDelay),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Retract to zero while paused: the first attempt never started, so start_delay is editable.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(0)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UnpauseActivityExecution(s.Context(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		// With the delay retracted, unpause dispatches immediately rather than waiting out the original 10s.
		pollCtx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		pollResp, err := env.pollActivityTaskQueue(pollCtx, taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken(),
			"activity must dispatch immediately after the start_delay is retracted")
	})

	// Not paused and past the delay window: the activity has been dispatched to matching (worker just
	// hasn't polled it), so start_delay is consumed and no longer editable.
	s.Run("UpdateAfterWindow_NotPaused_Rejected", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		startDelay := 1 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			StartDelay:          durationpb.New(startDelay),
		})
		require.NoError(t, err)

		// Do not poll: the activity dispatches to matching at startDelay but no worker starts it.
		// 2s > startDelay (1s) so firstDispatchTime is firmly in the past.
		time.Sleep(2 * time.Second) //nolint:forbidigo

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{StartDelay: durationpb.New(60 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}},
		})
		require.Error(t, err)
		var failedPrecond *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecond)
		require.Contains(t, failedPrecond.Message, "start_delay")
	})
}

func (s *standaloneActivityTestSuite) TestUpdateActivityExecutionOptions() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("InvalidArgument", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		})
		require.NoError(t, err)
		runID := startResp.RunId
		ns := env.Namespace().String()

		validOptions := &activitypb.ActivityOptions{
			StartToCloseTimeout: durationpb.New(2 * time.Minute),
		}
		validMask := &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout"}}

		testCases := []struct {
			name        string
			req         *workflowservice.UpdateActivityExecutionOptionsRequest
			expectedErr string
		}{
			{
				name: "EmptyActivityID",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					RunId:           runID,
					ActivityOptions: validOptions,
					UpdateMask:      validMask,
				},
				expectedErr: "activity ID is required",
			},
			{
				name: "ActivityIDTooLong",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      string(make([]byte, defaultMaxIDLengthLimit+1)),
					RunId:           runID,
					ActivityOptions: validOptions,
					UpdateMask:      validMask,
				},
				expectedErr: "activity ID exceeds length limit",
			},
			{
				name: "IdentityTooLong",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      activityID,
					RunId:           runID,
					Identity:        string(make([]byte, defaultMaxIDLengthLimit+1)),
					ActivityOptions: validOptions,
					UpdateMask:      validMask,
				},
				expectedErr: "identity exceeds length limit",
			},
			{
				name: "InvalidRunID",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      activityID,
					RunId:           "not-a-valid-uuid",
					ActivityOptions: validOptions,
					UpdateMask:      validMask,
				},
				expectedErr: "invalid run id",
			},
			{
				name: "EmptyUpdateMask",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      activityID,
					RunId:           runID,
					ActivityOptions: validOptions,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{}},
				},
				expectedErr: "UpdateMask",
			},
			{
				name: "UnknownPath",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      activityID,
					RunId:           runID,
					ActivityOptions: validOptions,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"definitely_not_an_activity_option"}},
				},
				expectedErr: "definitely_not_an_activity_option",
			},
			{
				name: "UnsupportedRootPath",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:  ns,
					ActivityId: activityID,
					RunId:      runID,
					ActivityOptions: &activitypb.ActivityOptions{
						TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"task_queue"}},
				},
				expectedErr: "task_queue",
			},
			{
				name: "WildcardPath",
				req: &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       ns,
					ActivityId:      activityID,
					RunId:           runID,
					ActivityOptions: validOptions,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"*"}},
				},
				expectedErr: "*",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, tc.req)
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, invalidArgErr.Message, tc.expectedErr)
			})
		}
	})

	t.Run("ChangeRetryInterval", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		// Start with a long retry interval to keep the activity in backoff after failure.
		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(10 * time.Minute),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		// Poll and fail with a retryable failure — activity enters long backoff.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		// Shorten the retry interval so the activity retries immediately.
		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp)

		// Activity should now be available to poll for attempt 2.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollResp2.GetTaskToken())
		require.EqualValues(t, 2, pollResp2.Attempt)
	})

	t.Run("ChangeRetryInterval_Consecutive", func(t *testing.T) {
		// Consecutive retry_policy updates in backoff: the second update must recalculate the
		// interval from the value the first update left behind, not the original interval.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		// Start with a long retry interval to keep the activity in backoff after failure.
		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(10 * time.Minute),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		// Poll and fail with a retryable failure — activity enters the 10-minute backoff.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		// Update 1: shorten to 5 minutes. Recalculates, but stays in backoff for the rest of the test.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(5 * time.Minute),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)

		// Update 2: shorten to 1ms. Must recalculate from update 1's interval so the retry fires now.
		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp)

		// Activity should now be available to poll for attempt 2 since we shortened it to 1ms.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollResp2.GetTaskToken())
		require.EqualValues(t, 2, pollResp2.Attempt)
	})

	t.Run("UpdateUnrelatedField_PreservesNextRetryDelay", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		nextRetryDelay := 2 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(10 * time.Minute),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable:   false,
						NextRetryDelay: durationpb.New(nextRetryDelay),
					},
				},
			},
		})
		require.NoError(t, err)

		// Updating an unrelated option must not recalculate the worker-provided next retry interval.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				HeartbeatTimeout: durationpb.New(5 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		// Backoff is the worker's 2s NextRetryDelay set during RespondActivityTaskFailed, not the policy's 10m, so 8s is enough to poll it.
		pollCtx, pollCancel := context.WithTimeout(ctx, 8*time.Second)
		defer pollCancel()
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotEmpty(t, pollResp2.GetTaskToken())
		require.EqualValues(t, 2, pollResp2.Attempt)
	})

	t.Run("UpdateRetryPolicy_PreservesEqualNextRetryDelay", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		workerNextRetryDelay := 2 * time.Minute
		updatedPolicyInterval := 10 * time.Minute

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(workerNextRetryDelay),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable:   false,
						NextRetryDelay: durationpb.New(workerNextRetryDelay),
					},
				},
			},
		})
		require.NoError(t, err)

		describeBeforeUpdate, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.NotNil(t, describeBeforeUpdate.GetInfo().GetCurrentRetryInterval())
		require.Equal(t, workerNextRetryDelay, describeBeforeUpdate.GetInfo().GetCurrentRetryInterval().AsDuration())
		require.NotNil(t, describeBeforeUpdate.GetInfo().GetNextAttemptScheduleTime())
		nextAttemptScheduleTime := describeBeforeUpdate.GetInfo().GetNextAttemptScheduleTime().AsTime()

		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(updatedPolicyInterval),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp)
		require.Equal(t, updatedPolicyInterval, updateResp.GetActivityOptions().GetRetryPolicy().GetInitialInterval().AsDuration())

		describeAfterUpdate, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, updatedPolicyInterval, describeAfterUpdate.GetInfo().GetRetryPolicy().GetInitialInterval().AsDuration())
		require.NotNil(t, describeAfterUpdate.GetInfo().GetCurrentRetryInterval())
		require.Equal(t, workerNextRetryDelay, describeAfterUpdate.GetInfo().GetCurrentRetryInterval().AsDuration())
		require.NotNil(t, describeAfterUpdate.GetInfo().GetNextAttemptScheduleTime())
		require.Equal(t, nextAttemptScheduleTime, describeAfterUpdate.GetInfo().GetNextAttemptScheduleTime().AsTime())
	})

	t.Run("ChangeRetryInterval_WhileStarted", func(t *testing.T) {
		// Update retry_policy.initial_interval while the activity is STARTED (running).
		// In this state CurrentRetryInterval is not recalculated at update time — the new
		// policy takes effect when the attempt fails and a fresh interval is computed.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(10 * time.Minute),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		// Poll attempt 1 — activity is now STARTED.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Shorten the retry interval while the activity is running.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)

		// Fail attempt 1 — next retry should dispatch immediately with the new 1ms interval.
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		// Attempt 2 should be available immediately.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)
	})

	t.Run("UpdateStartedActivityKeepsOriginalTaskTokenUsable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			HeartbeatTimeout:    durationpb.New(30 * time.Minute),
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		token, err := tasktoken.NewSerializer().Deserialize(pollResp.GetTaskToken())
		require.NoError(t, err)
		require.NotZero(t, token.GetActivityAttemptStamp())

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			ActivityOptions: &activitypb.ActivityOptions{
				HeartbeatTimeout: durationpb.New(20 * time.Minute),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("NilRetryPolicyOnStartUsesDefaultRetryPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy:         nil,
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)
		require.NotNil(t, pollResp.GetRetryPolicy())
		require.EqualValues(t, 0, pollResp.GetRetryPolicy().GetMaximumAttempts())

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus())
		require.EqualValues(t, 2, descResp.GetInfo().GetAttempt())
		require.NotNil(t, descResp.GetInfo().GetRetryPolicy())
		require.EqualValues(t, 0, descResp.GetInfo().GetRetryPolicy().GetMaximumAttempts())
	})

	t.Run("ClearRetryPolicyNormalizesToDefaultRetryPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		defaultRetryInitialInterval := 10 * time.Minute
		cleanup := env.OverrideDynamicConfig(dynamicconfig.DefaultActivityRetryPolicy, retrypolicy.DefaultRetrySettings{
			InitialInterval:            defaultRetryInitialInterval,
			MaximumIntervalCoefficient: 100.0,
			BackoffCoefficient:         2.0,
			MaximumAttempts:            0,
		})
		defer cleanup()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1,
			},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp.GetActivityOptions().GetRetryPolicy())
		require.Equal(t, defaultRetryInitialInterval, updateResp.GetActivityOptions().GetRetryPolicy().GetInitialInterval().AsDuration())
		require.EqualValues(t, 0, updateResp.GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp.GetInfo().GetRunState())
		require.EqualValues(t, 2, descResp.GetInfo().GetAttempt())
		require.NotNil(t, descResp.GetInfo().GetRetryPolicy())
		require.Equal(t, defaultRetryInitialInterval, descResp.GetInfo().GetRetryPolicy().GetInitialInterval().AsDuration())
		require.EqualValues(t, 0, descResp.GetInfo().GetRetryPolicy().GetMaximumAttempts())

		pollCtx, pollCancel := context.WithTimeout(ctx, common.MinLongPollTimeout+time.Second)
		defer pollCancel()
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		if err == nil {
			require.Empty(t, pollResp2.GetActivityId(), "retry should not dispatch immediately after normalizing nil retry policy to default backoff")
		} else {
			require.True(t, common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
		}
	})

	t.Run("ChangeScheduleToClose", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 0,                                // unlimited retries
				InitialInterval: durationpb.New(10 * time.Minute), // long backoff keeps activity in SCHEDULED state
			},
		})
		require.NoError(t, err)

		// Start and fail the activity once — it enters backoff (SCHEDULED state).
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		// Shorten schedule-to-close — activity should time out immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
		})
		require.NoError(t, err)

		// Long-poll until the activity times out.
		pollOutcome, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		)
	})

	t.Run("ChangeScheduleToClose_WhileStarted", func(t *testing.T) {
		// Poll the activity (STARTED), then shorten schedule-to-close timeout via update.
		// Since no response is sent, the activity should time out with SCHEDULE_TO_CLOSE.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // no retry so TIMED_OUT is terminal
			},
		})
		require.NoError(t, err)

		// Poll attempt 1 — activity is now STARTED.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Shorten schedule-to-close — the new task fires almost immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(1 * time.Millisecond),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
		})
		require.NoError(t, err)

		// Long-poll until the activity times out (no response sent to the task queue).
		pollOutcome, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		)
	})

	t.Run("ChangeScheduleToCloseAndRetry", func(t *testing.T) {
		// Start with a short schedule-to-close (8s) and a long retry interval (5s) so
		// the activity would time out before its second attempt under the original options.
		// Update both: longer schedule-to-close and shorter retry interval.
		// The activity should retry quickly and succeed.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalStartToClose := 8 * time.Second

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(originalStartToClose),
			ScheduleToCloseTimeout: durationpb.New(8 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(5 * time.Second),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		// Fail attempt 1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)

		// Update: extend schedule-to-close, shorten retry interval.
		newScheduleToClose := 30 * time.Second
		updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(newScheduleToClose),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}},
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp)
		require.Equal(t, int64(newScheduleToClose.Seconds()), updateResp.GetActivityOptions().GetScheduleToCloseTimeout().GetSeconds())
		// Verify that the unmodified start_to_close_timeout is preserved in the response.
		require.Equal(t, int64(originalStartToClose.Seconds()), updateResp.GetActivityOptions().GetStartToCloseTimeout().GetSeconds())

		// Attempt 2 should be available immediately.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)
	})

	t.Run("ResetDefaultOptions", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		originalMaxAttempts := int32(10)

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Millisecond),
				MaximumAttempts: originalMaxAttempts,
			},
		})
		require.NoError(t, err)

		// Update maximum attempts to a large value.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{MaximumAttempts: 1000},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}},
		})
		require.NoError(t, err)

		// Verify the update was applied.
		describeResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1000, describeResp.GetInfo().GetRetryPolicy().GetMaximumAttempts())

		// Reset to original options.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			RestoreOriginal: true,
		})
		require.NoError(t, err)

		// Verify original maximum attempts are restored.
		describeResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, originalMaxAttempts, describeResp.GetInfo().GetRetryPolicy().GetMaximumAttempts())

		// Verify the activity still executes after reset — poll attempt 1 and complete it.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    payloads.EncodeString("done"),
		})
		require.NoError(t, err)
	})

	t.Run("ChangeScheduleToStart", func(t *testing.T) {
		// Start activity with a long schedule-to-start timeout and no workers polling the task
		// queue. Shorten the timeout via update — activity should time out with SCHEDULE_TO_START.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // no retry so we observe TIMED_OUT
			},
		})
		require.NoError(t, err)

		// Shorten schedule-to-start — no workers are polling so it should fire immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToStartTimeout: durationpb.New(1 * time.Millisecond),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_start_timeout"}},
		})
		require.NoError(t, err)

		// Long-poll until the activity times out.
		pollOutcome, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		)
	})

	t.Run("ChangeStartToClose", func(t *testing.T) {
		// Poll the activity (STARTED), then shorten start-to-close timeout via update.
		// Since no response is sent, the activity should time out with START_TO_CLOSE.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // no retry so TIMED_OUT is terminal
			},
		})
		require.NoError(t, err)

		// Poll attempt 1 — activity is now STARTED.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Shorten start-to-close — the new task fires almost immediately.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(1 * time.Millisecond),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout"}},
		})
		require.NoError(t, err)

		// Long-poll until the activity times out (no response sent to the task queue).
		pollOutcome, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t,
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		)
	})

	t.Run("ChangeTaskQueue_RedispatchesToNewQueue", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		queueA := testcore.RandomizeStr(t.Name() + "-A")
		queueB := testcore.RandomizeStr(t.Name() + "-B")

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: queueA},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RetryPolicy:         &commonpb.RetryPolicy{MaximumAttempts: 1},
		})
		require.NoError(t, err)

		// Update to queue B before any worker poll.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{TaskQueue: &taskqueuepb.TaskQueue{Name: queueB}},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"task_queue.name"}},
		})
		require.NoError(t, err)

		// Poller on queue B picks it up.
		pollResp, err := env.pollActivityTaskQueue(ctx, queueB)
		require.NoError(t, err)
		require.Equal(t, activityID, pollResp.GetActivityId(), "task was not dispatched from the updated task queue")
		require.EqualValues(t, 1, pollResp.GetAttempt())

		// Describe reports queue B.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t, queueB, descResp.GetInfo().GetTaskQueue())

		// There are no tasks on queue A.
		shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
		defer shortCancel()
		oldQueueResp, err := env.pollActivityTaskQueue(shortCtx, queueA)
		if err == nil {
			require.Empty(t, oldQueueResp.GetActivityId(), "task should not be dispatchable from the old task queue after update")
		}
	})

	t.Run("ChangeHeartbeatTimeout", func(t *testing.T) {
		// Poll the activity (STARTED), then shorten heartbeat timeout via update.
		// The update re-creates the HeartbeatTimeoutTask with the new timeout, so no further
		// heartbeats are needed — the activity should time out with HEARTBEAT.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			HeartbeatTimeout:    durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 1, // no retry so TIMED_OUT is terminal
			},
		})
		require.NoError(t, err)

		// Poll attempt 1 — activity is now STARTED.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Shorten heartbeat timeout.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				HeartbeatTimeout: durationpb.New(2 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		// Long-poll until the activity times out (no further heartbeats sent).
		pollOutcome, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		require.Equal(t,
			enumspb.TIMEOUT_TYPE_HEARTBEAT,
			pollOutcome.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		)
	})

	t.Run("RejectInvalidMergedRetryPolicy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		// Start with a valid retry policy. The subfield updates below are validated against
		// the merged result, not just the incoming delta.
		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(30 * time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(10 * time.Second),
				MaximumAttempts: 5,
			},
		})
		require.NoError(t, err)

		testCases := []struct {
			name        string
			options     *activitypb.ActivityOptions
			mask        []string
			expectedErr string
		}{
			{
				// 1s is a valid duration on its own; it is invalid only because the merged
				// policy would have MaximumInterval (1s) < the existing InitialInterval (10s).
				name:        "MaximumIntervalBelowInitialInterval",
				options:     &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{MaximumInterval: durationpb.New(1 * time.Second)}},
				mask:        []string{"retry_policy.maximum_interval"},
				expectedErr: "MaximumInterval cannot be less than InitialInterval",
			},
			{
				name:        "BackoffCoefficientBelowOne",
				options:     &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{BackoffCoefficient: 0.5}},
				mask:        []string{"retry_policy.backoff_coefficient"},
				expectedErr: "BackoffCoefficient cannot be less than 1",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       env.Namespace().String(),
					ActivityId:      activityID,
					RunId:           startResp.RunId,
					ActivityOptions: tc.options,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: tc.mask},
				})
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, invalidArgErr.Message, tc.expectedErr)
			})
		}
	})

}

func (env *standaloneActivityEnv) pollActivityTaskQueue(ctx context.Context, taskQueue string) (*workflowservice.PollActivityTaskQueueResponse, error) {
	return env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: defaultIdentity,
	})
}

func (env *standaloneActivityEnv) startAndValidateActivity(
	ctx context.Context,
	t *testing.T,
	activityID string,
	taskQueue string,
) *workflowservice.StartActivityExecutionResponse {
	startResponse, err := env.startActivity(ctx, activityID, taskQueue)

	require.NoError(t, err)
	require.NotNil(t, startResponse.GetRunId())
	require.True(t, startResponse.Started)

	activityResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      env.Namespace().String(),
		ActivityId:     activityID,
		RunId:          startResponse.RunId,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	env.validateBaseActivityResponse(t, activityID, startResponse.RunId, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
		"expected Running but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState(),
		"expected Scheduled but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Nil(t, activityResp.Outcome)
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())

	return startResponse
}

func (env *standaloneActivityEnv) pollActivityTaskAndValidate(
	ctx context.Context,
	t *testing.T,
	activityID string,
	taskQueue string,
	runID string,
) *workflowservice.PollActivityTaskQueueResponse {
	pollTaskResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)
	require.Equal(t, activityID, pollTaskResp.GetActivityId())
	protorequire.ProtoEqual(t, env.Tv().ActivityType(), pollTaskResp.GetActivityType())
	require.EqualValues(t, 1, pollTaskResp.Attempt)

	activityResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      env.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	env.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.GetStatus(),
		"expected Running but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState(),
		"expected Started but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Equal(t, env.Tv().WorkerIdentity(), info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, activityResp.Outcome)
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())

	return pollTaskResp
}

func (env *standaloneActivityEnv) validateCompletion(
	ctx context.Context,
	t *testing.T,
	activityID string,
	runID string,
	workerIdentity string,
) {
	activityResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:      env.Namespace().String(),
		ActivityId:     activityID,
		RunId:          runID,
		IncludeInput:   true,
		IncludeOutcome: true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	env.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, info.GetStatus(),
		"expected Completed but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
		"expected Unspecified but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, info.GetCloseTime().AsTime().IsZero())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	require.Nil(t, info.GetLastFailure())
	require.Nil(t, info.GetHeartbeatDetails())
	require.Nil(t, info.GetLastHeartbeatTime())
	protorequire.ProtoEqual(t, defaultResult, activityResp.GetOutcome().GetResult())
}

func (env *standaloneActivityEnv) validateFailure(
	ctx context.Context,
	t *testing.T,
	activityID string,
	runID string,
	expectedHeartbeatDetails *commonpb.Payloads,
	workerIdentity string,
) {
	activityResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:               env.Namespace().String(),
		ActivityId:              activityID,
		RunId:                   runID,
		IncludeInput:            true,
		IncludeOutcome:          true,
		IncludeHeartbeatDetails: expectedHeartbeatDetails != nil,
		IncludeLastFailure:      true,
	})

	info := activityResp.GetInfo()

	require.NoError(t, err)
	env.validateBaseActivityResponse(t, activityID, runID, activityResp)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, info.GetStatus(),
		"expected Failed but is %s", info.GetStatus())
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, info.GetRunState(),
		"expected Unspecified but is %s", info.GetRunState())
	require.EqualValues(t, 1, info.GetAttempt())
	require.Greater(t, info.GetExecutionDuration().AsDuration(), time.Duration(0))
	require.False(t, info.GetCloseTime().AsTime().IsZero())
	require.Equal(t, workerIdentity, info.GetLastWorkerIdentity())
	require.NotNil(t, info.GetLastStartedTime())
	protorequire.ProtoEqual(t, defaultFailure, info.GetLastFailure())
	protorequire.ProtoEqual(t, defaultFailure, activityResp.GetOutcome().GetFailure())

	if expectedHeartbeatDetails != nil {
		protorequire.ProtoEqual(t, expectedHeartbeatDetails, info.GetHeartbeatDetails())
	} else {
		require.Nil(t, info.GetHeartbeatDetails())
	}
}

func (env *standaloneActivityEnv) validateBaseActivityResponse(
	t *testing.T,
	activityID string,
	expectedRunID string,
	response *workflowservice.DescribeActivityExecutionResponse,
) {
	require.NotNil(t, response.LongPollToken)
	require.Equal(t, activityID, response.GetInfo().GetActivityId())
	require.Equal(t, env.Tv().ActivityType(), response.GetInfo().GetActivityType())
	require.Equal(t, expectedRunID, response.RunId)
	require.NotNil(t, response.GetInfo().GetScheduleTime())
	protorequire.ProtoEqual(t, defaultInput, response.GetInput())
}

func (env *standaloneActivityEnv) startActivity(ctx context.Context, activityID string, taskQueue string) (*workflowservice.StartActivityExecutionResponse, error) {
	return env.startActivityWithType(ctx, activityID, taskQueue, env.Tv().ActivityType())
}

func (env *standaloneActivityEnv) startActivityWithType(ctx context.Context, activityID string, taskQueue string, activityType *commonpb.ActivityType) (*workflowservice.StartActivityExecutionResponse, error) {
	return env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    env.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: activityType,
		Identity:     env.Tv().WorkerIdentity(),
		Input:        defaultInput,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
		},
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RequestId:           env.Tv().RequestID(),
	})
}

func (env *standaloneActivityEnv) runNexusCompletionHTTPServer(t *testing.T, h *completionHandler) string {
	hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h})
	srv := httptest.NewServer(hh)
	t.Cleanup(func() {
		srv.Close()
	})
	return srv.URL
}

func (s *standaloneActivityTestSuite) TestCallbacks() {
	env := s.newTestEnv()
	t := s.T()

	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	t.Run("AcceptedOnStart", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		resp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://localhost/callback",
					},
				},
			}},
		})
		require.NoError(t, err)
		require.True(t, resp.Started)
		require.NotEmpty(t, resp.RunId)
	})

	t.Run("MultipleCallbacksAccepted", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		resp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback1"}}},
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback2"}}},
			},
		})
		require.NoError(t, err)
		require.True(t, resp.Started)
		require.NotEmpty(t, resp.RunId)
	})

	t.Run("DescribeIncludesCallbackInfo", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		callbackURL := "http://localhost/describe-callback"
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackURL}}},
			},
		})
		require.NoError(t, err)

		describeResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)

		require.Len(t, describeResp.Callbacks, 1)
		cbInfo := describeResp.Callbacks[0]
		require.NotNil(t, cbInfo.GetTrigger().GetActivityClosed())
		require.Equal(t, callbackURL, cbInfo.GetInfo().GetCallback().GetNexus().GetUrl())
		require.Equal(t, enumspb.CALLBACK_STATE_STANDBY, cbInfo.GetInfo().GetState())
		require.NotNil(t, cbInfo.GetInfo().GetRegistrationTime())
	})

	t.Run("ExceedsMaxCallbacksLimit", func(t *testing.T) {
		maxCallbacks := 1
		env.OverrideDynamicConfig(
			callback.MaxPerExecution,
			maxCallbacks,
		)

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			// Two callbacks when overridden max dynamic config is 1, so should error.
			CompletionCallbacks: []*commonpb.Callback{
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback1"}}},
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback2"}}},
			},
		})
		require.Error(t, err)
		require.ErrorContains(t, err, fmt.Sprintf("cannot attach more than %d callbacks", maxCallbacks))
	})

	t.Run("CompletesWithCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		ch := &completionHandler{
			requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
			requestCompleteCh: make(chan error, 1),
		}
		defer func() {
			close(ch.requestCh)
			close(ch.requestCompleteCh)
		}()
		callbackAddress := env.runNexusCompletionHTTPServer(t, ch)

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress}},
			}},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify the callback was actually delivered with the correct result.
		select {
		case completion := <-ch.requestCh:
			require.Equal(t, nexus.OperationStateSucceeded, completion.State)
			require.False(t, completion.StartTime.IsZero())
			require.False(t, completion.CloseTime.IsZero())
			body, readErr := io.ReadAll(completion.HTTPRequest.Body)
			_ = completion.HTTPRequest.Body.Close()
			require.NoError(t, readErr)
			require.JSONEq(t, string(defaultResult.Payloads[0].Data), string(body))
			// Unblock CompleteOperation so it returns 200 OK to the callback library
			ch.requestCompleteCh <- nil
		case <-s.Context().Done():
			require.Fail(t, "timed out waiting for completion callback")
		}

		// Verify the activity is in completed state.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
	})

	t.Run("FailsWithCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		ch := &completionHandler{
			requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
			requestCompleteCh: make(chan error, 1),
		}
		defer func() {
			close(ch.requestCh)
			close(ch.requestCompleteCh)
		}()
		callbackAddress := env.runNexusCompletionHTTPServer(t, ch)

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress}},
			}},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure:   defaultFailure,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify the callback was actually delivered with failure state.
		select {
		case completion := <-ch.requestCh:
			require.Equal(t, nexus.OperationStateFailed, completion.State)
			require.False(t, completion.StartTime.IsZero())
			require.False(t, completion.CloseTime.IsZero())
			var failureErr *nexus.FailureError
			require.ErrorAs(t, completion.Error.Cause, &failureErr)
			tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
			require.NoError(t, convErr)
			sdkErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, sdkErr, &appErr)
			require.Equal(t, defaultFailure.Message, appErr.Message())
			ch.requestCompleteCh <- nil
		case <-s.Context().Done():
			require.Fail(t, "timed out waiting for completion callback")
		}

		// Verify the activity is in failed state.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
	})

	t.Run("TerminatedWithCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		ch := &completionHandler{
			requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
			requestCompleteCh: make(chan error, 1),
		}
		defer func() {
			close(ch.requestCh)
			close(ch.requestCompleteCh)
		}()
		callbackAddress := env.runNexusCompletionHTTPServer(t, ch)

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress}},
			}},
		})
		require.NoError(t, err)
		runID := startResp.RunId

		_, err = env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		reason := "Test Termination"
		_, err = env.FrontendClient().TerminateActivityExecution(s.Context(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Reason:     reason,
			Identity:   "terminator",
		})
		require.NoError(t, err)

		// Verify the callback was delivered with failure state (terminated maps to failed).
		select {
		case completion := <-ch.requestCh:
			require.Equal(t, nexus.OperationStateFailed, completion.State)
			require.False(t, completion.StartTime.IsZero())
			require.False(t, completion.CloseTime.IsZero())
			var failureErr *nexus.FailureError
			require.ErrorAs(t, completion.Error.Cause, &failureErr)
			tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
			require.NoError(t, convErr)
			sdkErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
			var termErr *temporal.TerminatedError
			require.ErrorAs(t, sdkErr, &termErr)
			ch.requestCompleteCh <- nil
		case <-s.Context().Done():
			require.Fail(t, "timed out waiting for completion callback")
		}

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	t.Run("CanceledWithCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		ch := &completionHandler{
			requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
			requestCompleteCh: make(chan error, 1),
		}
		defer func() {
			close(ch.requestCh)
			close(ch.requestCompleteCh)
		}()
		callbackAddress := env.runNexusCompletionHTTPServer(t, ch)

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress}},
			}},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RequestCancelActivityExecution(s.Context(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "cancelling-worker",
			RequestId:  env.Tv().Any().String(),
			Reason:     "Test Cancellation",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RespondActivityTaskCanceled(s.Context(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify the callback was delivered with canceled state.
		select {
		case completion := <-ch.requestCh:
			require.Equal(t, nexus.OperationStateCanceled, completion.State)
			require.False(t, completion.StartTime.IsZero())
			require.False(t, completion.CloseTime.IsZero())
			var failureErr *nexus.FailureError
			require.ErrorAs(t, completion.Error.Cause, &failureErr)
			tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
			require.NoError(t, convErr)
			sdkErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
			var canceledErr *temporal.CanceledError
			require.ErrorAs(t, sdkErr, &canceledErr)
			ch.requestCompleteCh <- nil
		case <-s.Context().Done():
			require.Fail(t, "timed out waiting for completion callback")
		}

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, descResp.GetInfo().GetStatus())
	})

	// This test covers the timeout callback path using schedule-to-start, but the callback behavior
	// is the same for all timeout types (schedule-to-close, start-to-close, heartbeat).
	t.Run("TimeoutWithCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		ch := &completionHandler{
			requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
			requestCompleteCh: make(chan error, 1),
		}
		defer func() {
			close(ch.requestCh)
			close(ch.requestCompleteCh)
		}()
		callbackAddress := env.runNexusCompletionHTTPServer(t, ch)

		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:    env.Namespace().String(),
			ActivityId:   activityID,
			ActivityType: env.Tv().ActivityType(),
			Identity:     env.Tv().WorkerIdentity(),
			Input:        defaultInput,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(1 * time.Second),
			RequestId:              env.Tv().Any().String(),
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress}},
			}},
		})
		require.NoError(t, err)

		// No worker polls — activity will time out waiting to be started.

		// Verify the callback is delivered with failure state and non-zero CloseTime.
		select {
		case completion := <-ch.requestCh:
			require.Equal(t, nexus.OperationStateFailed, completion.State)
			var failureErr *nexus.FailureError
			require.ErrorAs(t, completion.Error.Cause, &failureErr)
			tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(failureErr.Failure)
			require.NoError(t, convErr)
			sdkErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
			var timeoutErr *temporal.TimeoutError
			require.ErrorAs(t, sdkErr, &timeoutErr)
			require.False(t, completion.StartTime.IsZero())
			require.False(t, completion.CloseTime.IsZero())
			ch.requestCompleteCh <- nil
		case <-s.Context().Done():
			require.Fail(t, "timed out waiting for completion callback")
		}

		// Verify the activity is in timed-out state.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, descResp.GetInfo().GetStatus())
	})
}

func (s *standaloneActivityTestSuite) TestCallbacksDisabled() {
	env := s.newTestEnv()
	t := s.T()

	// Override EnableCallbacks back to false to simulate a namespace where the feature is off.
	nsValues := []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: false},
	}
	env.GetTestCluster().OverrideDynamicConfig(t, activity.EnableCallbacks, nsValues)

	cb := []*commonpb.Callback{{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}},
	}}

	t.Run("StartWithCallbacksFails", func(t *testing.T) {
		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          testcore.RandomizeStr(t.Name()),
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr(t.Name())},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: cb,
		})
		require.ErrorContains(t, err, "completion callbacks are not enabled for this namespace")
	})

	t.Run("OnConflictAttachCallbacksFails", func(t *testing.T) {
		_, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          testcore.RandomizeStr(t.Name()),
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: testcore.RandomizeStr(t.Name())},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           env.Tv().Any().String(),
			CompletionCallbacks: cb,
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			OnConflictOptions:   &commonpb.OnConflictOptions{AttachCompletionCallbacks: true},
		})
		require.ErrorContains(t, err, "completion callbacks are not enabled for this namespace")
	})
}

func (s *standaloneActivityTestSuite) runNexusCompletionHTTPServer(t *testing.T, h *completionHandler) string {
	hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h})
	srv := httptest.NewServer(hh)
	t.Cleanup(func() {
		srv.Close()
	})
	return srv.URL
}

func (s *standaloneActivityTestSuite) TestPauseActivityExecution() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("PauseWhileStarted", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// DescribeActivityExecution should reflect PAUSE_REQUESTED run state: the activity is still
		// STARTED (worker token valid) but a pause has been requested via the flag.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())

		// Heartbeat should report ActivityPaused=true.
		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetActivityPaused(), "expected ActivityPaused=true after pause")

		// DescribeActivityExecution should still reflect PAUSE_REQUESTED.
		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())
	})

	t.Run("PauseWhileScheduled", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// DescribeActivityExecution should reflect PAUSED run state.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED, descResp.GetInfo().GetStatus())

		// Attempt to poll — the dispatch task was invalidated by the stamp bump, so no task should
		// be available. Use a short-lived context to avoid blocking the test.
		shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
		defer shortCancel()
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(shortCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		// Either the poll times out (deadline exceeded) or returns an empty response.
		if err == nil {
			require.Empty(t, pollResp.GetActivityId(), "expected no task to be dispatched while paused")
		}
	})

	t.Run("PauseWhilePaused", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		pauseReq := &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		}
		_, err := env.FrontendClient().PauseActivityExecution(ctx, pauseReq)
		require.NoError(t, err)

		// Second pause should fail with FailedPrecondition (activity is already paused).
		_, err = env.FrontendClient().PauseActivityExecution(ctx, pauseReq)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("PauseWhilePausedIdempotent", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		pauseReq := &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
			RequestId:  "some-request-id",
		}
		_, err := env.FrontendClient().PauseActivityExecution(ctx, pauseReq)
		require.NoError(t, err)

		resp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, resp.GetInfo().GetRunState())

		// Second pause with the same request ID should succeed (idempotent no-op).
		// Note there is no way to
		_, err = env.FrontendClient().PauseActivityExecution(ctx, pauseReq)
		require.NoError(t, err)

		resp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, resp.GetInfo().GetRunState())
	})

	t.Run("PauseReplayAfterUnpause_IsDeduplicated", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		const replayedRequestID = "at-008-replayed-request-id"

		// Original pause.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
			RequestId:  replayedRequestID,
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())

		// Unpause. The activity resumes and is no longer paused.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.NotEqual(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())

		// Delayed replay of the ORIGINAL pause request (same request ID), arriving after the
		// Unpause has already been applied. It must be deduplicated as a no-op, not re-pause
		// the activity.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
			RequestId:  replayedRequestID,
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.NotEqual(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState(),
			"replayed pause request with an already-consumed request ID must not re-pause the activity")
	})

	t.Run("PauseNotFound", func(t *testing.T) {
		ctx := testcore.NewContext()

		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: testcore.RandomizeStr(t.Name()),
			Identity:   "test-identity",
			Reason:     "test",
		})
		require.Error(t, err)
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
	})

	t.Run("PauseTerminalState", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Poll and complete the activity so it reaches a terminal state.
		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Pause should fail with FailedPrecondition on a terminal activity.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test",
		})
		require.Error(t, err)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	// PauseWhileRunning: pause a STARTED activity, fail the attempt, then verify the activity
	// stays paused (SCHEDULED + paused = RunState PAUSED) and is not dispatched until unpause.
	t.Run("PauseWhileRunning", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll – activity is now STARTED at attempt=1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Pause while STARTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-reason",
		})
		require.NoError(t, err)

		// Heartbeat should report ActivityPaused=true.
		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetActivityPaused())

		// Describe should show PAUSE_REQUESTED: a pause was requested while the worker was running.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())

		// Fail the attempt – this triggers a retry (attempt=2) but the activity stays paused.
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// After fail, activity should be PAUSED (SCHEDULED + paused) at attempt=2 with a recorded failure.
		await.Require(s.Context(), s.T(), func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:          env.Namespace().String(),
				ActivityId:         activityID,
				IncludeLastFailure: true,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
			require.NotNil(c, dr.GetInfo().GetLastFailure())
		}, 10*time.Second, 200*time.Millisecond)

		// Unpause – activity should be dispatched.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// Poll and complete the second attempt.
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, poll2Resp.GetActivityId())
		require.EqualValues(t, 2, poll2Resp.Attempt)
	})

	// PauseIncreaseAttemptsOnFailure: verify that attempt count increases and LastFailure is populated
	// when an activity fails while paused.
	t.Run("PauseIncreaseAttemptsOnFailure", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll – attempt=1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Pause while STARTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-reason",
		})
		require.NoError(t, err)

		// Describe should show PAUSE_REQUESTED: a pause was requested while the worker was running.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())
		require.EqualValues(t, 1, descResp.GetInfo().GetAttempt())

		failureMsg := "activity-failed-while-paused"
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: failureMsg,
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Verify attempt is now 2, activity is still paused, and LastFailure is populated.
		await.Require(s.Context(), t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:          env.Namespace().String(),
				ActivityId:         activityID,
				IncludeLastFailure: true,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
			require.Equal(c, failureMsg, dr.GetInfo().GetLastFailure().GetMessage())
		}, 10*time.Second, 200*time.Millisecond)

		// Unpause and complete.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, poll2Resp.GetActivityId())
		require.EqualValues(t, 2, poll2Resp.Attempt)
	})

	// PauseWhileWaiting: pause an activity that has already failed and is waiting in retry backoff
	// (SCHEDULED). Verify it is not dispatched while paused, then unpause and poll.
	// Uses a 30s retry interval (matching PauseWhileRetryNoWait) to eliminate the race between
	// "confirm attempt==2" and "pause call" — with 30s backoff the dispatch task cannot fire in
	// the window between those two operations.
	t.Run("PauseWhileWaiting", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(30 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt=1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Wait for the activity to be rescheduled at attempt=2 (in retry backoff).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		// Pause while in SCHEDULED retry backoff.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-reason",
		})
		require.NoError(t, err)

		// Verify activity is PAUSED at attempt=2 (not dispatched while paused).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		// Unpause – activity should be dispatched.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// Poll and complete attempt=2.
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, poll2Resp.GetActivityId())
		require.EqualValues(t, 2, poll2Resp.Attempt)
	})

	// PauseWhileRetryHonorsBackoff: pausing an activity mid retry-backoff and immediately unpausing
	// must not shortcut the remaining backoff. Unpause honors the pending retry's dispatch time
	// (complete_time + retry_interval), just as it honors a pending start_delay, so the activity
	// stays SCHEDULED until the original backoff would have elapsed rather than dispatching at once.
	t.Run("PauseWhileRetryHonorsBackoff", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(30 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt=1 – activity enters a 30s retry backoff.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Wait for activity to be rescheduled at attempt=2 (in retry backoff).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		// Pause, then immediately unpause.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// Honor backoff: the task must not be pollable before the retry deadline.
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		if err != nil {
			require.True(t, common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
		} else {
			require.Empty(
				t,
				poll2Resp.GetActivityId(),
				"unpause must honor the remaining retry backoff; activity task should not be pollable",
			)
		}
	})

	// After unpause, force dispatch reissue with an options update. The retry backoff should still
	// be preserved, so no activity task should be pollable before the original retry deadline.
	t.Run("PauseWhileRetryBackoffSurvivesOptionsUpdateAfterUnpause", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(30 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(
				c.Context(),
				&workflowservice.DescribeActivityExecutionRequest{
					Namespace:  env.Namespace().String(),
					ActivityId: activityID,
					RunId:      startResp.GetRunId(),
				})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(
			ctx,
			&workflowservice.UpdateActivityExecutionOptionsRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
				ActivityOptions: &activitypb.ActivityOptions{
					HeartbeatTimeout: durationpb.New(time.Second),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
			})
		require.NoError(t, err)

		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		if err != nil {
			require.True(t, common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
		} else {
			require.Empty(t, poll2Resp.GetActivityId(), "options update after unpause must not lose the pending retry backoff")
		}
	})

	// PauseWhileCancelRequested: pausing a CANCEL_REQUESTED activity must be rejected with
	// FailedPrecondition — cancellation takes precedence and cannot be overridden by a pause.
	t.Run("PauseWhileCancelRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Poll so the activity is STARTED.
		env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Request cancellation — activity transitions to CANCEL_REQUESTED.
		_, err := env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		// Pause must be rejected — cannot pause an activity with a pending cancellation.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.Error(t, err)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	// CancelWhilePaused: mirrors workflow activity behavior — cancelling a PAUSED activity
	// succeeds and takes effect immediately (no worker token is active).
	t.Run("CancelWhilePaused", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Pause while SCHEDULED → activity becomes PAUSED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Cancel should succeed and take effect immediately (no worker to notify).
		_, err = env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		// Activity should be CANCELED immediately.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, dr.GetInfo().GetStatus())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// CancelWhilePauseRequested: cancelling a STARTED activity that has a pending pause
	// (PAUSE_REQUESTED) is accepted and recorded — cancel takes precedence over the pending pause.
	// The worker is still running, so it is deferred (CANCEL_REQUESTED), not immediate.
	t.Run("CancelWhilePauseRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId
		env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Pause the STARTED activity → PAUSE_REQUESTED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, "test-cancel", descResp.GetInfo().GetCanceledReason(),
			"cancel should be recorded even with a pending pause")
	})

	// CancelWhileResetRequested: cancelling a STARTED activity that has a pending reset
	// (RESET_REQUESTED) is accepted and recorded — cancel takes precedence; deferred (CANCEL_REQUESTED).
	t.Run("CancelWhileResetRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId
		env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Reset the STARTED activity → RESET_REQUESTED.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, "test-cancel", descResp.GetInfo().GetCanceledReason(),
			"cancel should be recorded even with a pending reset")
	})

	// PauseWhileResetRequested: pausing a STARTED activity that has a pending reset (RESET_REQUESTED)
	// is rejected — RESET_REQUESTED is a non-pausable state.
	t.Run("PauseWhileResetRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId
		env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.Error(t, err)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	// UpdateWhileCancelRequested: updating options on a STARTED activity that has a pending cancel
	// (CANCEL_REQUESTED) is allowed — the worker is still running and the update applies to it.
	t.Run("UpdateWhileCancelRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId
		env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		_, err := env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           runID,
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(15 * time.Second)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, 15*time.Second, descResp.GetInfo().GetHeartbeatTimeout().AsDuration(),
			"update should apply to the running attempt even with a pending cancel")
	})

	// TerminateWhilePaused: design doc says PAUSED + terminate → TERMINATED.
	t.Run("TerminateWhilePaused", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Pause while SCHEDULED → PAUSED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())

		// Terminate while PAUSED → TERMINATED.
		_, err = env.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Reason:     "test-terminate",
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	// ScheduleToCloseTimeoutWhilePaused: design doc says PAUSED + S2C timeout → TIMED_OUT.
	// The S2C task is created with a.Stamp (not attempt.Stamp), and TransitionPaused only bumps
	// attempt.Stamp, so the S2C task remains valid and fires even while the activity is PAUSED.
	t.Run("ScheduleToCloseTimeoutWhilePaused", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
			RequestId:              env.Tv().RequestID(),
		})
		require.NoError(t, err)
		runID := startResp.RunId

		// Pause immediately while SCHEDULED — the S2C timer is still running.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Long-poll for the activity outcome — the S2C timeout fires while the activity is PAUSED.
		pollActivityResp, err := env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			pollActivityResp.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType(),
			"expected ScheduleToCloseTimeout while paused")
	})

	// CompleteWhileStartedAndPaused: design doc says STARTED + paused + worker completes → COMPLETED.
	// The PauseWhileRunning test only covers the fail+retry path; this covers the success path.
	t.Run("CompleteWhileStartedAndPaused", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Poll → STARTED.
		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Pause while STARTED → PAUSE_REQUESTED (flag-only; worker token stays valid).
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())

		// Worker completes despite the pause flag — pause is advisory, the token is still valid.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Activity must be COMPLETED.
		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
	})

	// NonRetryableFailWhilePaused: design doc says STARTED + paused + non-retryable fail → FAILED.
	t.Run("NonRetryableFailWhilePaused", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 10,
			},
		})
		require.NoError(t, err)

		// Poll → STARTED.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Pause while STARTED → PAUSE_REQUESTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Worker fails with a non-retryable error — must transition to FAILED, not retry or stay paused.
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "non-retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
		require.EqualValues(t, 1, descResp.GetInfo().GetAttempt(), "non-retryable fail must not increment attempt")
	})

	// StartToCloseTimeoutWhilePauseRequested: a STARTED activity with a pending pause must still
	// observe its StartToClose timeout. The worker stops responding, the timer fires, the retry
	// path consumes the pause-request and lands the activity in PAUSED with the attempt count
	// incremented.
	t.Run("StartToCloseTimeoutWhilePauseRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Second),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Worker polls → activity is STARTED.
		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Pause while STARTED → PAUSE_REQUESTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Worker stops responding. StartToCloseTimeout must fire and the pause-request must be
		// consumed by a retry, landing the activity in PAUSED at attempt 2.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// HeartbeatTimeoutWhilePauseRequested: as StartToCloseTimeoutWhilePauseRequested but for the
	// heartbeat timer.
	t.Run("HeartbeatTimeoutWhilePauseRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			HeartbeatTimeout:       durationpb.New(1 * time.Second),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// No heartbeat → HeartbeatTimeoutTask fires → retry consumes pause-request → PAUSED at attempt 2.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// UpdateOptionsPreservesTimeoutsWhilePauseRequested: UpdateActivityExecutionOptions bumps the
	// attempt stamp, which invalidates all attempt-scoped timeout tasks. The handler must re-emit
	// fresh StartToClose and Heartbeat timeout tasks for PAUSE_REQUESTED activities, otherwise the
	// running worker is left with no server-side timeout enforcement (only the long
	// ScheduleToCloseTimeout would eventually catch a hung worker).
	t.Run("UpdateOptionsPreservesTimeoutsWhilePauseRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Pause while STARTED → PAUSE_REQUESTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Update StartToCloseTimeout to 1s. The handler bumps the attempt stamp (invalidating the
		// old 1-minute timeout task) and must re-emit a fresh 1-second timeout task that fires
		// while the activity is PAUSE_REQUESTED.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(1 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout"}},
		})
		require.NoError(t, err)

		// New StartToCloseTimeout fires → retry consumes pause-request → PAUSED at attempt 2.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// ResetKeepPausedWhilePauseRequested: a reset with KeepPaused=true on a PAUSE_REQUESTED
	// activity must defer the reset (via ActivityReset) and have it consumed by the next retry,
	// landing the activity in PAUSED at attempt 1. Pins the contract between handleReset and
	// AttemptFailedWhilePauseRequested.
	t.Run("ResetKeepPausedWhilePauseRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(1 * time.Second),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Drive the activity to attempt 2 by polling then failing attempt 1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "fail attempt 1",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "TestFailure",
				}},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Worker polls attempt 2 → STARTED at attempt 2.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 5*time.Second, 100*time.Millisecond)
		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Pause while STARTED at attempt 2 → PAUSE_REQUESTED.
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Reset with KeepPaused=true. Reset is deferred via ActivityReset; status stays PAUSE_REQUESTED.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: true,
		})
		require.NoError(t, err)

		// Worker stops responding. StartToCloseTimeout fires → AttemptFailedWhilePauseRequested
		// consumes ActivityReset → PAUSED at attempt 1.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 1, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// PauseRequestValidation: validate that Pause rejects invalid request fields.
	t.Run("PauseRequestValidation", func(t *testing.T) {
		ctx := testcore.NewContext()

		t.Run("EmptyActivityID", func(t *testing.T) {
			_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-identity",
				Reason:    "test-pause",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
				Identity:   "test-identity",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("ReasonTooLong", func(t *testing.T) {
			blobSizeLimitError := 1000
			cleanup := env.OverrideDynamicConfig(
				dynamicconfig.BlobSizeLimitError,
				blobSizeLimitError,
			)
			defer cleanup()

			_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				Identity:   "test-identity",
				Reason:     string(make([]byte, blobSizeLimitError+1)),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Identity:   "test-identity",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})
	})

	// PauseUpdateOptionsAndUnpause: pause an activity while it's in retry backoff, update the
	// retry interval while paused, then unpause and verify the update took effect — the activity
	// is dispatched immediately (short interval) and runs to completion.
	t.Run("PauseUpdateOptionsAndUnpause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(10 * time.Minute),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll and fail attempt 1 — activity enters the 10-minute retry backoff.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Wait for attempt 2 (rescheduled in long backoff, not yet dispatched).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		// Pause while SCHEDULED (in 10-minute retry backoff).
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Update retry interval to 1ms while paused.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Millisecond),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		require.NoError(t, err)

		// Verify the update was persisted while the activity remains paused.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, descResp.GetInfo().GetRunState())
		require.Equal(t, durationpb.New(1*time.Millisecond), descResp.GetInfo().GetRetryPolicy().GetInitialInterval())

		// Unpause — the shortened interval means the activity is dispatched immediately.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// Poll attempt 2 — available immediately because the retry interval is now 1ms.
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, poll2Resp.GetActivityId())
		require.EqualValues(t, 2, poll2Resp.Attempt)

		// Complete the activity.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: poll2Resp.TaskToken,
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Verify terminal COMPLETED state with updated retry policy.
		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
		require.Equal(t, durationpb.New(1*time.Millisecond), descResp.GetInfo().GetRetryPolicy().GetInitialInterval())
	})
}

func (s *standaloneActivityTestSuite) TestUnpauseActivityExecution() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("UnpauseWhileScheduled", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Pause while SCHEDULED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Unpause — this should re-dispatch the activity.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// Poll should now succeed.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, pollResp.GetActivityId(), "expected activity to be dispatched after unpause")
	})

	t.Run("UnpauseWhileStarted", func(t *testing.T) {
		ctx := testcore.NewContext()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Pause while STARTED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetActivityPaused(), "expected ActivityPaused=true after pause")

		// Unpause.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		// After unpause of a PAUSE_REQUESTED activity, the status goes back to STARTED (the worker's
		// token is still valid — no stamp bump). Verify via describe that the activity is no longer paused.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, descResp.GetInfo().GetRunState(),
			"expected activity to be STARTED after unpause")
	})

	t.Run("UnpauseIdempotent", func(t *testing.T) {
		ctx := testcore.NewContext()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Unpause a non-paused activity — should succeed with no error.
		_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)
	})

	t.Run("UnpauseWithResetAttempts", func(t *testing.T) {
		ctx := testcore.NewContext()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(30 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll and fail the first attempt to advance the attempt count.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Wait for the activity to enter SCHEDULED state for retry.
		await.Require(ctx, t, func(c *await.T) {
			descResp, descErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, descErr)
			require.EqualValues(c, 2, descResp.GetInfo().GetAttempt())
		}, 15*time.Second, 200*time.Millisecond)

		// Pause while SCHEDULED (attempt=2).
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Unpause with ResetAttempts=true.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:     env.Namespace().String(),
			ActivityId:    activityID,
			Identity:      "test-identity",
			ResetAttempts: true,
		})
		require.NoError(t, err)

		// Force dispatch reissue before polling. ResetAttempts must clear the old retry backoff,
		// otherwise this update would re-delay the reset attempt.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(
			ctx,
			&workflowservice.UpdateActivityExecutionOptionsRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
				ActivityOptions: &activitypb.ActivityOptions{
					HeartbeatTimeout: durationpb.New(time.Second),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
			})
		require.NoError(t, err)

		// Poll: attempt count should be reset to 1 without waiting for the old 30s retry backoff.
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, poll2Resp.Attempt, "expected attempt reset to 1 after UnpauseWithResetAttempts")
	})

	t.Run("UnpauseWithJitter", func(t *testing.T) {
		ctx := testcore.NewContext()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Jitter:     durationpb.New(500 * time.Millisecond),
		})
		require.NoError(t, err)

		// Activity should eventually be dispatched despite the jitter.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, pollResp.GetActivityId())
	})

	t.Run("UnpauseNotFound", func(t *testing.T) {
		ctx := testcore.NewContext()

		_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: testcore.RandomizeStr(t.Name()),
			Identity:   "test-identity",
		})
		require.Error(t, err)
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)
	})

	t.Run("UnpauseTerminalState", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Poll and complete the activity so it reaches a terminal state.
		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Unpause should fail with FailedPrecondition on a terminal activity.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.Error(t, err)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	// UnpauseWithResetHeartbeat: verify that unpause with reset_heartbeat=true clears heartbeat
	// details recorded during a prior attempt.
	t.Run("UnpauseWithResetHeartbeat", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
			RequestId:           env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(30 * time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Poll attempt 1.
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp.Attempt)

		// Record a heartbeat with details.
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   defaultHeartbeatDetails,
		})
		require.NoError(t, err)

		// Fail attempt 1 — activity enters 30s retry backoff at attempt 2.
		_, err = env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Wait for attempt 2 (count increments immediately on reschedule, even during backoff).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.EqualValues(c, 2, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)

		// Heartbeat details should still be set before the unpause.
		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.NotNil(t, descResp.GetInfo().GetLastHeartbeatTime(), "expected heartbeat time before unpause")
		require.NotNil(t, descResp.GetInfo().GetHeartbeatDetails(), "expected heartbeat details before unpause")

		// Pause while SCHEDULED (in 30s backoff).
		_, err = env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Unpause with ResetHeartbeat=true — clears the recorded heartbeat state.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     activityID,
			Identity:       "test-identity",
			ResetHeartbeat: true,
		})
		require.NoError(t, err)

		// Heartbeat details must be cleared after unpause.
		descResp, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.Nil(t, descResp.GetInfo().GetLastHeartbeatTime(),
			"expected heartbeat time cleared after UnpauseWithResetHeartbeat")
		require.Nil(t, descResp.GetInfo().GetHeartbeatDetails(),
			"expected heartbeat details cleared after UnpauseWithResetHeartbeat")

		// Poll attempt 2 — heartbeat details must be nil in the poll response too.
		poll2Resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)
		require.Equal(t, activityID, poll2Resp.GetActivityId())
		require.Nil(t, poll2Resp.GetHeartbeatDetails(), "expected nil heartbeat details in poll after reset")
	})

	// Issuing Unpause on a CANCEL_REQUESTED activity is a no-op.
	t.Run("UnpauseWhileCancelRequested", func(t *testing.T) {
		ctx := testcore.NewContext()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		runID := startResp.RunId

		// Poll → STARTED.
		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)

		// Pause while STARTED → PAUSE_REQUESTED.
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		// Cancel → CANCEL_REQUESTED.
		_, err = env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "test-cancel",
			RequestId:  env.Tv().RequestID(),
		})
		require.NoError(t, err)

		// Heartbeat shows cancel-requested; pause is no longer surfaced.
		hbResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, hbResp.GetCancelRequested())
		require.False(t, hbResp.GetActivityPaused())

		// Unpause is a no-op: status stays CANCEL_REQUESTED.
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, descResp.GetInfo().GetRunState(),
			"unpause of a CANCEL_REQUESTED activity must be a no-op")

		hbResp2, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, hbResp2.GetCancelRequested(), "cancel must remain after unpause")
		require.False(t, hbResp2.GetActivityPaused(), "pause flag must remain cleared")
	})

	// UnpauseRequestValidation: validate that Unpause rejects invalid request fields.
	t.Run("UnpauseRequestValidation", func(t *testing.T) {
		ctx := testcore.NewContext()

		t.Run("EmptyActivityID", func(t *testing.T) {
			_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-identity",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "activity ID is required", invalidArgErr.Message)
		})

		t.Run("ActivityIDTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
				Identity:   "test-identity",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("IdentityTooLong", func(t *testing.T) {
			_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)),
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
				defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
		})

		t.Run("InvalidRunID", func(t *testing.T) {
			_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: testcore.RandomizeStr(t.Name()),
				RunId:      "invalid-run-id",
				Identity:   "test-identity",
			})
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
		})
	})
}

func (s *standaloneActivityTestSuite) TestResetActivityExecution() {
	env := s.newTestEnv()
	t := s.T()

	// startAndPollActivity starts a SAA, polls for the first task, and returns
	// the start response, poll response, and the task queue name used.
	startAndPollActivity := func(ctx context.Context, t *testing.T, activityID string, retryPolicy *commonpb.RetryPolicy) (
		*workflowservice.StartActivityExecutionResponse,
		*workflowservice.PollActivityTaskQueueResponse,
		string,
	) {
		t.Helper()
		taskQueue := testcore.RandomizeStr(t.Name())
		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            defaultIdentity,
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(15 * time.Minute),
			RetryPolicy:         retryPolicy,
			RequestId:           testcore.RandomizeStr(activityID),
		})
		require.NoError(t, err)

		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.Equal(t, activityID, pollResp.GetActivityId())
		return startResp, pollResp, taskQueue
	}

	pollActivity := func(ctx context.Context, t *testing.T, taskQueue string) *workflowservice.PollActivityTaskQueueResponse {
		t.Helper()
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		return pollResp
	}

	failAttemptRetryably := func(ctx context.Context, t *testing.T, taskToken []byte, nextRetryDelay time.Duration) {
		t.Helper()
		_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: taskToken,
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable:   false,
						NextRetryDelay: durationpb.New(nextRetryDelay),
					},
				},
			},
			Identity: defaultIdentity,
		})
		require.NoError(t, err)
	}

	failAttemptNonRetryably := func(ctx context.Context, t *testing.T, taskToken []byte) {
		t.Helper()
		_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: taskToken,
			Failure: &failurepb.Failure{
				Message: "non-retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: true,
					},
				},
			},
			Identity: defaultIdentity,
		})
		require.NoError(t, err)
	}

	completeAttempt := func(ctx context.Context, t *testing.T, taskToken []byte) {
		t.Helper()
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Result:    defaultResult,
			Namespace: env.Namespace().String(),
			TaskToken: taskToken,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	}

	resetActivity := func(ctx context.Context, t *testing.T, activityID, runID string) {
		t.Helper()
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
	}

	resetActivityRestoreOriginalOptions := func(ctx context.Context, t *testing.T, activityID, runID string) {
		t.Helper()
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  runID,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)
	}

	pauseActivity := func(ctx context.Context, t *testing.T, activityID, runID string) {
		t.Helper()
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   defaultIdentity,
			Reason:     "test-pause",
		})
		require.NoError(t, err)
	}

	unpauseActivity := func(ctx context.Context, t *testing.T, activityID, runID string) {
		t.Helper()
		_, err := env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)
	}

	describeActivity := func(ctx context.Context, t *testing.T, activityID, runID string) *workflowservice.DescribeActivityExecutionResponse {
		t.Helper()
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		return desc
	}

	waitForState := func(ctx context.Context, t *testing.T, activityID, runID string, state enumspb.PendingActivityState) {
		t.Helper()
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      runID,
			})
			require.NoError(c, err)
			require.Equal(c, state, desc.GetInfo().GetRunState())
		}, 5*time.Second, 100*time.Millisecond)
	}

	t.Run("AfterRetry", func(t *testing.T) {
		// Start activity, let it fail twice (attempt 3 backing off with long interval),
		// then reset. Verify the next attempt starts at 1.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Fail attempt 1 with a short retry
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, time.Second)

		// Poll attempt 2
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, pollResp2.Attempt)

		// Fail attempt 2 with a long backoff so the activity is SCHEDULED waiting
		failAttemptRetryably(ctx, t, pollResp2.TaskToken, 60*time.Second)

		// Verify activity is SCHEDULED (backing off at attempt 3)
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.NotNil(c, desc.GetInfo())
			info := desc.GetInfo()
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
			require.EqualValues(c, 3, info.GetAttempt())
		}, 5*time.Second, 200*time.Millisecond)

		// Reset while SCHEDULED — should re-dispatch immediately at attempt 1
		resetActivity(ctx, t, activityID, startResp.GetRunId())

		// Poll — should be attempt 1
		pollResp3, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp3.Attempt, "attempt should be reset to 1")

		// Complete successfully
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp3.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("TokenFromBeforeResetCannotCompleteResetAttempt", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Minute),
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)
		require.EqualValues(t, 1, pollResp1.Attempt)

		originalToken, err := tasktoken.NewSerializer().Deserialize(pollResp1.GetTaskToken())
		require.NoError(t, err)
		require.NotZero(t, originalToken.GetActivityAttemptStamp())

		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState())
			require.EqualValues(c, 2, desc.GetInfo().GetAttempt())
		}, 5*time.Second, 200*time.Millisecond)

		resetActivity(ctx, t, activityID, startResp.GetRunId())

		pollResp2 := pollActivity(ctx, t, taskQueue)
		require.EqualValues(t, 1, pollResp2.Attempt)

		resetToken, err := tasktoken.NewSerializer().Deserialize(pollResp2.GetTaskToken())
		require.NoError(t, err)
		require.NotZero(t, resetToken.GetActivityAttemptStamp())
		require.NotEqual(t, originalToken.GetActivityAttemptStamp(), resetToken.GetActivityAttemptStamp())

		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Identity:  defaultIdentity,
		})
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.ErrorAs(t, err, &notFoundErr)

		completeAttempt(ctx, t, pollResp2.TaskToken)
	})

	t.Run("WhileRunning", func(t *testing.T) {
		// Reset while the activity is STARTED. The reset is deferred to the next
		// retry — the running attempt fails normally, then retries at attempt 1.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Reset while running
		resetActivity(ctx, t, activityID, startResp.GetRunId())

		// Verify activity still appears as STARTED (reset is deferred)
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.GetInfo().GetRunState())

		// Fail the running attempt — triggers deferred reset in TransitionRescheduled
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Poll the retry — should be attempt 1
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1 on retry after running reset")

		// Complete
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	// Resetting a STARTED activity that is on attempt > 1 must re-dispatch the fresh attempt 1
	// promptly: reset honors start_delay (here 0, so immediate) but must NOT honor the retry
	// backoff of the attempt that was in flight.
	t.Run("WhileRunningOnLaterAttemptDropsRetryBackoff", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 10.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Fail attempt 1 -> ~1s backoff, then attempt 2 is dispatched and picked up by a worker.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)
		pollResp2 := pollActivity(ctx, t, taskQueue)
		require.EqualValues(t, 2, pollResp2.Attempt)

		// Reset while attempt 2 is STARTED (deferred), then fail attempt 2 to trigger the reset.
		resetActivity(ctx, t, activityID, startResp.GetRunId())
		failAttemptRetryably(ctx, t, pollResp2.TaskToken, 0)

		// The reset attempt (attempt 1) must dispatch promptly.
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		pollResp3, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err, "reset attempt should dispatch promptly, not after the in-flight attempt's retry backoff")
		require.NotEmpty(t, pollResp3.GetTaskToken(), "reset attempt should dispatch promptly, not after the in-flight attempt's retry backoff")
		require.EqualValues(t, 1, pollResp3.Attempt, "attempt should be reset to 1")

		completeAttempt(ctx, t, pollResp3.TaskToken)
	})

	// Reset(keepPaused=true) of a STARTED activity on attempt > 1 lands it back in PAUSED via
	// TransitionResetAttemptFailedToPaused.
	t.Run("KeepPausedWhileRunningOnLaterAttemptDropsRetryBackoff", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 10.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Fail attempt 1 -> ~1s backoff, then attempt 2 is dispatched and picked up by a worker.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)
		pollResp2 := pollActivity(ctx, t, taskQueue)
		require.EqualValues(t, 2, pollResp2.Attempt)

		// Pause the running attempt (-> PAUSE_REQUESTED), then reset with keepPaused so the activity
		// lands back in PAUSED (not SCHEDULED) when the attempt yields.
		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: true,
		})
		require.NoError(t, err)

		// Fail attempt 2 -> deferred keep-paused reset -> PAUSED at attempt 1.
		failAttemptRetryably(ctx, t, pollResp2.TaskToken, 0)
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSED)

		// Unpause: attempt 1 must dispatch promptly, not after attempt 2's 10s backoff.
		unpauseActivity(ctx, t, activityID, startResp.GetRunId())

		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		pollResp3, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err, "unpaused reset attempt should dispatch promptly, not after the in-flight attempt's retry backoff")
		require.NotEmpty(t, pollResp3.GetTaskToken(), "unpaused reset attempt should dispatch promptly, not after the in-flight attempt's retry backoff")
		require.EqualValues(t, 1, pollResp3.Attempt, "attempt should be reset to 1")

		completeAttempt(ctx, t, pollResp3.TaskToken)
	})

	t.Run("WhileCancelRequestedReturnsFailedPrecondition", func(t *testing.T) {
		// Reset on a CANCEL_REQUESTED activity is rejected: cancel takes precedence and the
		// state machine has no meaningful transition for "reset while cancel is pending".
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, _ := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Request cancellation — moves to CANCEL_REQUESTED.
		_, err := env.FrontendClient().RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Identity:   defaultIdentity,
			RequestId:  testcore.RandomizeStr(activityID),
		})
		require.NoError(t, err)

		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, desc.GetInfo().GetRunState())

		// Reset while CANCEL_REQUESTED — must be rejected.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.Error(t, err)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition,
			"reset on CANCEL_REQUESTED activity must return FailedPrecondition")

		// Activity is still CANCEL_REQUESTED and can still complete via the existing token.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("InRetryWithLongInterval", func(t *testing.T) {
		// Activity is backing off for a long interval. Reset re-dispatches immediately.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Minute), // long backoff
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Fail attempt 1 — now backing off for 1 minute
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Verify in SCHEDULED state
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState())
		}, 5*time.Second, 200*time.Millisecond)

		// Reset — should bypass the 1-minute wait and dispatch immediately
		resetActivity(ctx, t, activityID, startResp.GetRunId())

		// Poll — task should be available immediately after reset (no long backoff)
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err, "should receive task quickly after reset (no long backoff)")
		require.EqualValues(t, 1, pollResp2.Attempt)

		// Complete
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("ResetClearsHeartbeatDetails", func(t *testing.T) {
		// Activity records heartbeats. Reset clears them.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Record a heartbeat
		_, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Details:   defaultHeartbeatDetails,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify heartbeat is visible in describe
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.GetRunId(),
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.NotNil(t, desc.GetInfo().GetHeartbeatDetails())

		// Fail the attempt with long backoff
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 60*time.Second)

		// Wait for SCHEDULED state
		await.Require(ctx, t, func(c *await.T) {
			d, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, d.GetInfo().GetRunState())
		}, 5*time.Second, 200*time.Millisecond)

		// Reset clears recorded heartbeat state.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)

		// Poll — attempt 1, no heartbeat details
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt)
		require.Empty(t, pollResp2.HeartbeatDetails.GetPayloads(), "heartbeat details should be cleared after reset")

		// Complete
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("ResetClearsHeartbeatState", func(t *testing.T) {
		// Reset while the activity is STARTED.
		// The heartbeat clear is deferred — it only takes effect on the next retry,
		// matching the behavior of the workflow activity HeartbeatDetails reset test.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Record a heartbeat while running
		_, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
			Details:   defaultHeartbeatDetails,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify heartbeat is visible
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.GetRunId(),
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.NotNil(t, desc.GetInfo().GetHeartbeatDetails())

		// Reset while STARTED — heartbeat clearing is deferred.
		resetActivity(ctx, t, activityID, startResp.GetRunId())

		// Activity should still be STARTED with heartbeat still visible (reset is deferred)
		desc, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.GetRunId(),
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.GetInfo().GetRunState())
		require.NotNil(t, desc.GetInfo().GetHeartbeatDetails(), "heartbeat should still be visible before the attempt fails")

		// Fail the running attempt — triggers deferred reset+heartbeat clear in TransitionRescheduled
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Poll retry — attempt=1, heartbeat details cleared
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1")
		require.Empty(t, pollResp2.HeartbeatDetails.GetPayloads(), "heartbeat details should be cleared after deferred reset")

		// Record a new heartbeat on the new attempt
		_, err = env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Details:   defaultHeartbeatDetails,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Verify new heartbeat is visible in describe
		desc, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:               env.Namespace().String(),
			ActivityId:              activityID,
			RunId:                   startResp.GetRunId(),
			IncludeHeartbeatDetails: true,
		})
		require.NoError(t, err)
		require.NotNil(t, desc.GetInfo().GetHeartbeatDetails(), "new heartbeat from reset attempt should be visible")

		// Complete
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("TerminalStateReturnsFailedPrecondition", func(t *testing.T) {
		// Resetting a completed activity should return FailedPrecondition.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp, _ := startAndPollActivity(ctx, t, activityID, nil)

		// Complete the activity
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)

		// Attempt to reset — should fail with FailedPrecondition since the activity is in a terminal state
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	})

	t.Run("KeepPaused", func(t *testing.T) {
		// Reset while activity is paused, with keepPaused=true.
		// Verifies that the activity remains paused after reset and that the attempt
		// count is reset to 1.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Minute), // long backoff so we can pause while scheduled
			BackoffCoefficient: 1.0,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Fail attempt 1 with a short override retry so it enters backoff
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Wait for SCHEDULED state (retry backoff)
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState())
		}, 5*time.Second, 200*time.Millisecond)

		// Pause the activity
		_, err := env.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Identity:   defaultIdentity,
			Reason:     "test pause before reset",
		})
		require.NoError(t, err)

		// Verify activity is paused
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.GetInfo().GetRunState())
		}, 5*time.Second, 200*time.Millisecond)

		// Verify attempt count is >= 2 (failed at least once before pause)
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Greater(t, desc.GetInfo().GetAttempt(), int32(1))

		// Reset with keepPaused=true — activity should remain paused but attempt reset to 1
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: true,
		})
		require.NoError(t, err)

		// Verify still paused with attempt=1
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.GetInfo().GetRunState())
			require.EqualValues(c, 1, desc.GetInfo().GetAttempt())
		}, 2*time.Second, 200*time.Millisecond)

		// Unpause the activity
		_, err = env.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Identity:   defaultIdentity,
		})
		require.NoError(t, err)

		// Poll — should be attempt 1 after unpause
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt)

		// Complete
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("ScheduledWithPauseStateKeepPausedFalse", func(t *testing.T) {
		// PAUSED activity (SCHEDULED status), reset with keepPaused=false.
		// Verify: attempt count reset to 1 and the activity dispatches (does not stay paused).
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})

		// Fail attempt 1 → SCHEDULED backoff.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_SCHEDULED)

		// Pause → PAUSED.
		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSED)

		// Reset with keepPaused=false — should dispatch at attempt 1 (not stay paused).
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: false,
		})
		require.NoError(t, err)

		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1")

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("StartedWithPauseStateKeepPausedFalse", func(t *testing.T) {
		// PAUSE_REQUESTED activity (STARTED status), reset with keepPaused=false.
		// Reset is deferred; without ResetKeepPaused the next retry dispatches instead of pausing.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Pause while STARTED → status becomes PAUSE_REQUESTED (worker still running).
		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED)

		// Reset with keepPaused=false — deferred; ResetKeepPaused stays false so dispatch isn't blocked.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: false,
		})
		require.NoError(t, err)

		// Fail the running attempt — triggers TransitionRescheduled with the deferred reset.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Activity should dispatch (not be stuck paused) at attempt 1.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1 after deferred reset")

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	t.Run("StartedWithPauseStateKeepPausedTrue", func(t *testing.T) {
		// PAUSE_REQUESTED activity (STARTED status), reset with keepPaused=true.
		// Reset is deferred; ResetKeepPaused is set so after the retry the activity stays paused.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})
		require.EqualValues(t, 1, pollResp1.Attempt)

		// Pause while STARTED.
		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED)

		// Reset with keepPaused=true — deferred; ResetKeepPaused should be set.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: true,
		})
		require.NoError(t, err)

		// Fail the running attempt.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		// Activity should be PAUSED at attempt 1 (deferred reset + preserved pause).
		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.GetInfo().GetRunState())
			require.EqualValues(c, 1, desc.GetInfo().GetAttempt())
		}, 5*time.Second, 100*time.Millisecond)

		// Unpause and verify dispatch at attempt 1.
		unpauseActivity(ctx, t, activityID, startResp.GetRunId())

		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt)

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	// ResetActivityExecution with keep_paused=true can arrive while a worker still owns
	// the current attempt. The reset itself must remain deferred until that worker
	// reports back, but a later unpause should clear the request to keep the reset
	// attempt paused.
	t.Run("UnpauseClearsKeepPausedButKeepsDeferredReset", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})
		require.EqualValues(t, 1, pollResp1.Attempt)

		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED)

		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			KeepPaused: true,
		})
		require.NoError(t, err)

		unpauseActivity(ctx, t, activityID, startResp.GetRunId())

		heartbeatResp, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.TaskToken,
		})
		require.NoError(t, err)
		require.True(t, heartbeatResp.GetActivityReset(), "reset should remain pending after unpause")
		require.False(t, heartbeatResp.GetActivityPaused(), "unpause should clear the pending keep-paused reset intent")

		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)

		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err, "activity should dispatch after the worker failure applies the pending reset")
		require.Equal(t, activityID, pollResp2.GetActivityId())
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1 after deferred reset")

		completeAttempt(ctx, t, pollResp2.TaskToken)
	})

	// startAttemptWithTimeouts starts a SAA with the given per-attempt timeouts and retry policy and
	// polls it to STARTED, returning the start response, the in-flight task token, and the task queue.
	startAttemptWithTimeouts := func(ctx context.Context, t *testing.T, activityID string, startToClose, heartbeat time.Duration) (
		*workflowservice.StartActivityExecutionResponse, []byte,
	) {
		t.Helper()
		taskQueue := testcore.RandomizeStr(t.Name())
		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            defaultIdentity,
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(startToClose),
			HeartbeatTimeout:    durationpb.New(heartbeat),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(10 * time.Second),
				BackoffCoefficient: 1.0,
				MaximumAttempts:    10,
			},
			RequestId: testcore.RandomizeStr(activityID),
		})
		require.NoError(t, err)
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		return startResp, pollResp.GetTaskToken()
	}

	updateTimeouts := func(ctx context.Context, t *testing.T, activityID, runID string, startToClose, heartbeat time.Duration) {
		t.Helper()
		_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			ActivityOptions: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(startToClose),
				HeartbeatTimeout:    durationpb.New(heartbeat),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout", "heartbeat_timeout"}},
		})
		require.NoError(t, err)
	}

	t.Run("RestoreOriginalOptions_WhileScheduled", func(t *testing.T) {
		// Start activity with specific options, update them, then reset with
		// RestoreOriginalOptions=true and verify the original options come back
		// along with the attempt count being reset to 1.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		originalMaxAttempts := int32(7)
		retryPolicy := &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
			MaximumAttempts:    originalMaxAttempts,
		}
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, retryPolicy)

		// Fail attempt 1 with a short backoff so the activity is SCHEDULED backing off (this test
		// covers option restoration, not backoff timing; the interval elapses before the reset so
		// the honored re-dispatch is immediate).
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, time.Second)

		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState())
		}, 5*time.Second, 100*time.Millisecond)

		// Update MaximumAttempts to a different value.
		updatedMaxAttempts := int32(100)
		_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			ActivityId:      activityID,
			RunId:           startResp.GetRunId(),
			ActivityOptions: &activitypb.ActivityOptions{RetryPolicy: &commonpb.RetryPolicy{MaximumAttempts: updatedMaxAttempts}},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}},
		})
		require.NoError(t, err)

		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, updatedMaxAttempts, desc.GetInfo().GetRetryPolicy().GetMaximumAttempts(), "update should be applied before reset")

		// Reset with RestoreOriginalOptions=true — options should revert and attempt reset to 1.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Verify original options are reflected in describe after reset.
		desc, err = env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, desc.GetInfo().GetAttempt(), "attempt should be reset to 1")
		require.Equal(t, originalMaxAttempts, desc.GetInfo().GetRetryPolicy().GetMaximumAttempts(), "original MaximumAttempts should be restored")

		// Poll — should be attempt 1.
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1")

		// Complete the activity.
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})

	// A Reset request when STARTED should be honored even if the attempt fails retryably.
	t.Run("RestoreOriginalOptions_WhileStarted_AttemptFailsRetryably", func(t *testing.T) {
		cases := []struct {
			name        string
			maxAttempts int32
		}{
			{"RetriesRemainingWhenApplyingPendingReset", 10},
			// The pending reset must be honored even if the "should retry" decision would otherwise be "no".
			{"RetriesExhaustedWhenApplyingPendingReset", 2},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				activityID := testcore.RandomizeStr(t.Name())

				// Start the first attempt, and then fail it so that we're on attempt 2
				startResp, pollResp, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(1 * time.Second),
					BackoffCoefficient: 1.0,
					MaximumAttempts:    tc.maxAttempts,
				})
				failAttemptRetryably(ctx, t, pollResp.TaskToken, 0)
				desc := describeActivity(ctx, t, activityID, startResp.GetRunId())
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState(), "expected in SCHEDULED")
				require.EqualValues(t, 2, desc.GetInfo().GetAttempt(), "expected attempt 2")
				originalTimeouts := []time.Duration{
					desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
					desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
				}
				updatedTimeouts := []time.Duration{
					originalTimeouts[0] + 1*time.Second,
					originalTimeouts[1] + 1*time.Second,
				}
				// Start the second attempt
				pollResp = pollActivity(ctx, t, taskQueue)
				require.EqualValues(t, 2, pollResp.Attempt)

				// Update some options
				updateTimeouts(ctx, t, activityID, startResp.GetRunId(), updatedTimeouts[0], updatedTimeouts[1])
				desc = describeActivity(ctx, t, activityID, startResp.GetRunId())
				require.Equal(t, updatedTimeouts[0], desc.GetInfo().GetStartToCloseTimeout().AsDuration())
				require.Equal(t, updatedTimeouts[1], desc.GetInfo().GetHeartbeatTimeout().AsDuration())

				// Reset(RestoreOriginals) -> RESET_REQUESTED
				resetActivityRestoreOriginalOptions(ctx, t, activityID, startResp.GetRunId())

				// Fail attempt retryably -> should reset
				failAttemptRetryably(ctx, t, pollResp.TaskToken, 0)

				// The reset should have been applied with the restore
				desc = describeActivity(ctx, t, activityID, startResp.GetRunId())
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState(), "expected in SCHEDULED")
				require.EqualValues(t, 1, desc.GetInfo().GetAttempt(), "expected attempt 1")
				require.Equal(t, originalTimeouts[0], desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
					"reset should have restored options")
				require.Equal(t, originalTimeouts[1], desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
					"reset should have restored options")
			})
		}
	})

	// A Reset request when STARTED should be honored even if the attempt fails non-retryably.
	t.Run("RestoreOriginalOptions_WhileStarted_AttemptFailsNonRetryably", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		originalTimeouts := []time.Duration{60 * time.Second, 50 * time.Second}
		updatedTimeouts := []time.Duration{30 * time.Second, 20 * time.Second}

		// Start the first attempt and then update the timeouts
		startResp, taskToken := startAttemptWithTimeouts(ctx, t, activityID, originalTimeouts[0], originalTimeouts[1])
		updateTimeouts(ctx, t, activityID, startResp.GetRunId(), updatedTimeouts[0], updatedTimeouts[1])

		// Reset(RestoreOriginals) -> RESET_REQUESTED
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Fail attempt non-retryably -> should still reset
		failAttemptNonRetryably(ctx, t, taskToken)

		// The reset should have been applied with the restore
		desc := describeActivity(ctx, t, activityID, startResp.GetRunId())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState(), "expected in SCHEDULED")
		require.EqualValues(t, 1, desc.GetInfo().GetAttempt(), "expected attempt 1")
		require.Equal(t, originalTimeouts[0], desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
			"reset should have restored options")
		require.Equal(t, originalTimeouts[1], desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
			"reset should have restored options")
	})

	t.Run("RestoreOriginalOptions_WhileStarted_AttemptSucceeds", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		originalTimeouts := []time.Duration{60 * time.Second, 50 * time.Second}
		updatedTimeouts := []time.Duration{30 * time.Second, 20 * time.Second}

		// Start the first attempt and then update the timeouts
		startResp, taskToken := startAttemptWithTimeouts(ctx, t, activityID, originalTimeouts[0], originalTimeouts[1])
		updateTimeouts(ctx, t, activityID, startResp.GetRunId(), updatedTimeouts[0], updatedTimeouts[1])

		// Reset(RestoreOriginals) -> RESET_REQUESTED
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Complete attempt -> Activity succeeds
		completeAttempt(ctx, t, taskToken)

		// The activity should have succeeded and the restore changes should never have been applied
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, desc.GetInfo().GetStatus())
		require.EqualValues(t, 1, desc.GetInfo().GetAttempt())
		require.Equal(t, updatedTimeouts[0], desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
			"completion with reset requested should not have restored options")
		require.Equal(t, updatedTimeouts[1], desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
			"completion with reset requested should not have restored options")
	})

	s.Run("RestoreOriginalOptions_OnStarted_DefersScheduleToCloseRestore", func(s *standaloneActivityTestSuite) {
		// Reset(RestoreOriginalOptions) on a STARTED attempt must NOT move the in-flight attempt's
		// ScheduleToClose deadline. Reset means "restart at attempt 1"; every restored option — including
		// the ScheduleToClose lifetime budget — takes effect only when the reset lands on the next
		// attempt, never on the attempt that happens to be running when the reset is issued.
		//
		// Setup: create with ScheduleToClose=2s (so the restored original is short). Start it, then extend
		// ScheduleToClose and StartToClose to 8s so the in-flight attempt is governed by 8s.
		// Reset(RestoreOriginalOptions) restores the 2s originals. If the restore leaked onto the in-flight
		// attempt the activity would time out at the 2s deadline; correct (deferred) behavior leaves the
		// running attempt on its current 8s budget.
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		})
		require.NoError(t, err)

		// Transition to STARTED; worker never responds
		_, err = env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)

		// Extend ScheduleToClose and StartToClose to 8s: the in-flight attempt is now governed by 8s.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(8 * time.Second),
				StartToCloseTimeout:    durationpb.New(8 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "start_to_close_timeout"}},
		})
		require.NoError(t, err)

		// Reset(RestoreOriginalOptions): restores ScheduleToClose to 2s. The restore must be deferred to
		// the reset landing, leaving the in-flight attempt on its current 8s deadline.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// The in-flight attempt must not time out at the restored 2s deadline. Buggy code re-arms
		// ScheduleToClose at 2s immediately → TIMED_OUT within ~2s; correct code leaves it at 8s.
		require.Never(t, func() bool {
			resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.RunId,
			})
			if err != nil {
				return false // a poll that loses the race to teardown must not fail the test
			}
			return resp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		}, 4*time.Second, 200*time.Millisecond,
			"in-flight attempt must keep its current 8s ScheduleToClose; RestoreOriginalOptions must defer the restore of the 2s original to the reset landing")
	})

	// Reset during a running attempt is a DEFERRED reset: the activity goes to RESET_REQUESTED, the
	// worker keeps running the in-flight attempt under its current terms, and the reset lands (attempt
	// count -> 1, re-dispatch) only when the worker yields. RestoreOriginalOptions must leave the running
	// attempt UNDISTURBED: EVERY restored option takes effect only when the reset lands on the next
	// attempt — none may be applied to, or reported for, the in-flight attempt. This includes the
	// ScheduleToClose lifetime budget (see ResetRestoreOriginal_OnStarted_DefersScheduleToCloseRestore
	// for the timer-firing proof) as well as RetryPolicy and Priority. start_delay is already skipped for
	// a started attempt (it only governs the first dispatch).
	//
	// The bug: the restore block mutates the option fields immediately, before entering RESET_REQUESTED,
	// so the reported options diverge from the values the in-flight attempt is actually governed by. The
	// discriminator here is the reported state during RESET_REQUESTED: it must still reflect the updated
	// (pre-restore) values, not the restored originals.
	s.Run("ResetRestoreOriginal_OnStarted_DefersPerAttemptOptionRestore", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		// Originals: long timeouts and a distinctive retry policy / priority, restored on the reset
		// landing. Updated to different values below; no timer fires during the test.
		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(120 * time.Second),
			StartToCloseTimeout:    durationpb.New(60 * time.Second),
			HeartbeatTimeout:       durationpb.New(50 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(10 * time.Second),
				BackoffCoefficient: 2.0,
				MaximumAttempts:    5,
			},
			Priority: &commonpb.Priority{PriorityKey: 1},
		})
		require.NoError(t, err)

		// Transition to STARTED; the worker never responds.
		pollResp, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.NotEmpty(t, pollResp.GetTaskToken())

		// Update every option: the in-flight attempt is now governed by these (updated) values.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(90 * time.Second),
				StartToCloseTimeout:    durationpb.New(30 * time.Second),
				HeartbeatTimeout:       durationpb.New(20 * time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(7 * time.Second),
					BackoffCoefficient: 3.0,
					MaximumAttempts:    9,
				},
				Priority: &commonpb.Priority{PriorityKey: 4},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
				"schedule_to_close_timeout", "start_to_close_timeout", "heartbeat_timeout", "retry_policy", "priority",
			}},
		})
		require.NoError(t, err)

		// Reset(RestoreOriginalOptions) -> RESET_REQUESTED. The in-flight attempt should be left undisturbed.
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// During RESET_REQUESTED, Describe must still report the updated (in-flight) values for every
		// option. The restored originals must not surface until the reset lands on the next attempt.
		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
		})
		require.NoError(t, err)
		got := descResp.GetInfo()
		require.Equal(t, 30*time.Second, got.GetStartToCloseTimeout().AsDuration(),
			"running attempt must keep its current StartToClose (30s); RestoreOriginalOptions must defer the restore until the reset lands")
		require.Equal(t, 20*time.Second, got.GetHeartbeatTimeout().AsDuration(),
			"running attempt must keep its current Heartbeat (20s); RestoreOriginalOptions must defer the restore until the reset lands")
		require.Equal(t, 90*time.Second, got.GetScheduleToCloseTimeout().AsDuration(),
			"running attempt must keep its current ScheduleToClose (90s); RestoreOriginalOptions must defer the restore until the reset lands")
		require.Equal(t, 7*time.Second, got.GetRetryPolicy().GetInitialInterval().AsDuration(),
			"running attempt must keep its current RetryPolicy; RestoreOriginalOptions must defer the restore until the reset lands")
		require.EqualValues(t, 9, got.GetRetryPolicy().GetMaximumAttempts(),
			"running attempt must keep its current RetryPolicy; RestoreOriginalOptions must defer the restore until the reset lands")
		require.EqualValues(t, 4, got.GetPriority().GetPriorityKey(),
			"running attempt must keep its current Priority; RestoreOriginalOptions must defer the restore until the reset lands")
	})

	// Companion to the two deferral tests above: once the worker yields and the reset LANDS on the next
	// attempt, the restored options must take effect. Deferral must not mean "dropped." (This passes both
	// before and after the in-flight-deferral fix; it guards the landing path.)
	s.Run("ResetRestoreOriginal_OnStarted_AppliesRestoredOptionsOnLanding", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(15 * time.Minute),
			StartToCloseTimeout:    durationpb.New(60 * time.Second),
			HeartbeatTimeout:       durationpb.New(50 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		pollResp1, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp1.GetAttempt())

		// Update the per-attempt timeouts for the in-flight attempt.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(s.Context(), &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(30 * time.Second),
				HeartbeatTimeout:    durationpb.New(20 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout", "heartbeat_timeout"}},
		})
		require.NoError(t, err)

		// Reset(RestoreOriginalOptions) while STARTED -> RESET_REQUESTED (restore deferred).
		_, err = env.FrontendClient().ResetActivityExecution(s.Context(), &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.RunId,
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Worker yields with a retryable failure -> the reset lands at attempt 1, applying the restore.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp1.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable:   false,
						NextRetryDelay: durationpb.New(0),
					},
				},
			},
			Identity: env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// The new attempt is dispatched at attempt 1 under the RESTORED per-attempt timeouts (60s / 50s).
		pollResp2, err := env.pollActivityTaskQueue(s.Context(), taskQueue)
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.GetAttempt(), "reset lands at attempt 1")
		require.Equal(t, 60*time.Second, pollResp2.GetStartToCloseTimeout().AsDuration(),
			"new attempt must run under the restored StartToClose (60s)")
		require.Equal(t, 50*time.Second, pollResp2.GetHeartbeatTimeout().AsDuration(),
			"new attempt must run under the restored Heartbeat (50s)")
	})

	// ResetRestoreOriginal_Started_AppliesDeferredRestoreOnReschedule covers the *apply* half of the
	// deferred per-attempt option restore. ResetRestoreOriginal_OnStarted_DefersPerAttemptOptionRestore
	// (in TestStartDelay) only checks that the restore is deferred while the attempt is in flight; this
	// drives the attempt to fail with retries remaining so the reset lands in SCHEDULED, and checks the
	// next attempt actually carries the restored timeouts. Without applyDeferredOptionRestore the next
	// attempt would keep the in-flight (updated) values.
	t.Run("ResetRestoreOriginal_Started_AppliesDeferredRestoreOnReschedule", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, taskToken := startAttemptWithTimeouts(ctx, t, activityID, 60*time.Second, 50*time.Second)

		// In-flight attempt is now governed by 30s/20s.
		updateTimeouts(ctx, t, activityID, startResp.GetRunId(), 30*time.Second, 20*time.Second)

		// Reset with RestoreOriginalOptions while running -> RESET_REQUESTED, restore deferred.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.NoError(t, err)

		// Worker yields with retries remaining -> reset lands in SCHEDULED at attempt 1 and the deferred
		// restore is applied, so the next attempt carries the original 60s/50s.
		failAttemptRetryably(ctx, t, taskToken, 0)

		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.GetInfo().GetRunState())
			require.EqualValues(c, 1, desc.GetInfo().GetAttempt())
			require.Equal(c, 60*time.Second, desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
				"next attempt must carry the restored StartToClose (60s); the deferred restore must be applied on the reset landing")
			require.Equal(c, 50*time.Second, desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
				"next attempt must carry the restored Heartbeat (50s)")
		}, 5*time.Second, 100*time.Millisecond)
	})

	// ResetRestoreOriginal_KeepPaused_AppliesDeferredRestoreOnPausedLanding is the same as the test
	// above but for the PAUSED landing: reset with KeepPaused on a paused (PAUSE_REQUESTED) running
	// attempt, then fail it. The reset lands in PAUSED via TransitionResetAttemptFailedToPaused, which
	// must also apply the deferred restore so the paused activity carries the original timeouts.
	t.Run("ResetRestoreOriginal_KeepPaused_AppliesDeferredRestoreOnPausedLanding", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, taskToken := startAttemptWithTimeouts(ctx, t, activityID, 60*time.Second, 50*time.Second)

		updateTimeouts(ctx, t, activityID, startResp.GetRunId(), 30*time.Second, 20*time.Second)

		// Pause the running attempt -> PAUSE_REQUESTED.
		pauseActivity(ctx, t, activityID, startResp.GetRunId())
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED)

		// Reset with RestoreOriginalOptions + KeepPaused -> RESET_REQUESTED, restore deferred, pause kept.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
			KeepPaused:             true,
		})
		require.NoError(t, err)

		// Worker yields -> reset lands in PAUSED at attempt 1 and the deferred restore is applied.
		failAttemptRetryably(ctx, t, taskToken, 0)

		await.Require(ctx, t, func(c *await.T) {
			desc, err := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
				RunId:      startResp.GetRunId(),
			})
			require.NoError(c, err)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.GetInfo().GetRunState())
			require.EqualValues(c, 1, desc.GetInfo().GetAttempt())
			require.Equal(c, 60*time.Second, desc.GetInfo().GetStartToCloseTimeout().AsDuration(),
				"paused next attempt must carry the restored StartToClose (60s)")
			require.Equal(c, 50*time.Second, desc.GetInfo().GetHeartbeatTimeout().AsDuration(),
				"paused next attempt must carry the restored Heartbeat (50s)")
		}, 5*time.Second, 100*time.Millisecond)
	})
	// ResetWhileResetRequested_Rejected: a reset issued while a reset is already pending (the
	// activity is RESET_REQUESTED, worker still running the attempt) is rejected with a clear
	// message and leaves the activity untouched. Accepting a second/overlapping reset is a separate
	// semantics question deferred for now; this pins the current behavior as an explicit rejection
	// rather than the misleading "activity execution is not running".
	t.Run("ResetWhileResetRequested_Rejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, _ := startAttemptWithTimeouts(ctx, t, activityID, 60*time.Second, 50*time.Second)

		// Change a per-attempt option so a leaked restore from the rejected reset would be detectable.
		updateTimeouts(ctx, t, activityID, startResp.GetRunId(), 30*time.Second, 20*time.Second)

		// First reset -> RESET_REQUESTED (deferred reset of the running attempt). The reset handler
		// runs synchronously, so the activity is in RESET_REQUESTED once this returns. (RESET_REQUESTED
		// surfaces externally as runState STARTED — the worker is still running its attempt.)
		resetActivity(ctx, t, activityID, startResp.GetRunId())

		// Second reset while a reset is already pending must be rejected with a clear message — the
		// worker is still running the attempt, so "activity execution is not running" would be wrong.
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			RunId:                  startResp.GetRunId(),
			RestoreOriginalOptions: true,
		})
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.Contains(t, err.Error(), "pending reset")

		// The rejected reset must leave the activity untouched: still running its attempt
		// (RESET_REQUESTED, surfaced as STARTED), options unchanged.
		desc, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.GetInfo().GetRunState())
		require.Equal(t, 30*time.Second, desc.GetInfo().GetStartToCloseTimeout().AsDuration())
		require.Equal(t, 20*time.Second, desc.GetInfo().GetHeartbeatTimeout().AsDuration())
	})

	// StartToCloseTimeoutWhileResetRequested: a STARTED activity with a pending reset must still
	// observe its StartToClose timeout. The worker stops responding, the timer fires, the retry
	// path consumes the reset-request and lands the activity in SCHEDULED with the attempt count
	// reset to 1.
	t.Run("StartToCloseTimeoutWhileResetRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Second),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		// Worker polls → activity is STARTED.
		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Reset while STARTED → RESET_REQUESTED.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)

		// Worker stops responding. StartToCloseTimeout must fire and the reset-request must be
		// consumed by a retry, landing the activity in SCHEDULED at attempt 1 (reset applied).
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 1, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// HeartbeatTimeoutWhileResetRequested: as above but for the heartbeat timer. Without the
	// validator accepting RESET_REQUESTED, the HeartbeatTimeoutTask is silently dropped and the
	// activity never times out (only the longer ScheduleToCloseTimeout would catch it).
	t.Run("HeartbeatTimeoutWhileResetRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			HeartbeatTimeout:       durationpb.New(1 * time.Second),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)

		// No heartbeat → HeartbeatTimeoutTask fires → retry consumes reset-request → SCHEDULED at attempt 1.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 1, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	// UpdateOptionsPreservesTimeoutsWhileResetRequested: UpdateActivityExecutionOptions bumps the
	// attempt stamp, which invalidates all attempt-scoped timeout tasks. The handler must re-emit
	// fresh StartToClose and Heartbeat timeout tasks for RESET_REQUESTED activities, otherwise the
	// running worker is left with no server-side timeout enforcement (only the long
	// ScheduleToCloseTimeout would eventually catch a hung worker).
	t.Run("UpdateOptionsPreservesTimeoutsWhileResetRequested", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(testcore.NewContext(), 30*time.Second)
		defer cancel()
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())

		startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			Identity:               env.Tv().WorkerIdentity(),
			Input:                  defaultInput,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout:    durationpb.New(1 * time.Minute),
			ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
			RequestId:              env.Tv().RequestID(),
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    10,
				InitialInterval:    durationpb.New(1 * time.Millisecond),
				BackoffCoefficient: 1.0,
			},
		})
		require.NoError(t, err)

		_, err = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  env.Tv().WorkerIdentity(),
		})
		require.NoError(t, err)

		// Reset while STARTED → RESET_REQUESTED.
		_, err = env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		require.NoError(t, err)

		// Update StartToCloseTimeout to 1s. The handler bumps the attempt stamp (invalidating the
		// old 1-minute timeout task) and must re-emit a fresh 1-second timeout task that fires
		// while the activity is RESET_REQUESTED.
		_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.RunId,
			ActivityOptions: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(1 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout"}},
		})
		require.NoError(t, err)

		// New StartToCloseTimeout fires → retry consumes reset-request → SCHEDULED at attempt 1.
		await.Require(ctx, t, func(c *await.T) {
			dr, dErr := env.FrontendClient().DescribeActivityExecution(c.Context(), &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: activityID,
			})
			require.NoError(c, dErr)
			require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, dr.GetInfo().GetRunState())
			require.EqualValues(c, 1, dr.GetInfo().GetAttempt())
		}, 10*time.Second, 200*time.Millisecond)
	})

	t.Run("Jitter", func(t *testing.T) {
		// A non-zero jitter should delay the dispatch task by at most jitter duration.
		// Verify the activity is not immediately available (still SCHEDULED briefly) and
		// then dispatches within the jitter window.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityID := testcore.RandomizeStr(t.Name())
		startResp, pollResp1, taskQueue := startAndPollActivity(ctx, t, activityID, &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
		})

		// Fail attempt 1 so the activity is SCHEDULED in retry backoff. Use a short retry interval
		// (policy default 1s) that elapses before the reset, so the honored re-dispatch floor is in
		// the past and the jitter governs the dispatch time.
		failAttemptRetryably(ctx, t, pollResp1.TaskToken, 0)
		waitForState(ctx, t, activityID, startResp.GetRunId(), enumspb.PENDING_ACTIVITY_STATE_SCHEDULED)

		jitter := 3 * time.Second
		resetStart := time.Now()
		_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      startResp.GetRunId(),
			Jitter:     durationpb.New(jitter),
		})
		require.NoError(t, err)

		// Activity should dispatch within [now, now+jitter+buffer].
		pollResp2, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, pollResp2.Attempt, "attempt should be reset to 1")
		require.WithinDuration(t, resetStart.Add(jitter), time.Now(), jitter+5*time.Second,
			"activity should dispatch within jitter window")

		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp2.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	})
}

// saaTraceBudget raises the parent test's context budget: the declarative trace subtests pay real
// wall-clock waits, so a group can run a few minutes past the default per-test timeout.
func saaTraceBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// runTrace drives one declared trace on its own harness (a unique activity-id namespace via idBase),
// making no behavioral assertions — see saaHarness.driveTrace — and returns a handle to the activity
// at the reached state so the caller can issue further RPCs and assert on the outcome. Later steps
// will add model-derived assertions during the drive itself.
func (s *standaloneActivityTestSuite) runTrace(t *testing.T, env *standaloneActivityEnv, tr saaTrace) *saaHandle {
	ctx := testcontext.For(t)
	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)
	h := &saaHarness{
		env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
		idBase:     testcore.RandomizeStr(t.Name()),
		cfg:        tr.config(),
		startDelay: tr.startDelay(), retryInterval: tr.retryInterval, nextRetryDelay: tr.nextRetryDelay,
		// A timeout's *Elapses event in the script is the signal to configure that timeout short.
		shortTimeout: saaTimeoutIn(tr.trace),
		// "Dispatchable" must mean "dispatches promptly", so bound the positive poll below the delay
		// window — that is how a reset that discards a backoff (immediate) is told from still-delayed.
		positivePollTimeout: saaNegativePollTimeout,
	}
	return h.driveTrace(t, tr.trace)
}

// TestStartDelay_Declarative drives the start-delay scenarios, each an explicitly named subtest with
// its trace declared inline. It only drives (no assertions yet); see runTrace / driveTrace.
func (s *standaloneActivityTestSuite) TestStartDelay_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("start-delay/first-dispatch", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{saaPoll, saaStartDelayElapse, saaPoll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/pause-then-unpause", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.Pause}, {Kind: model.Unpause}, saaPoll, saaStartDelayElapse, saaPoll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/reset", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.Reset}, saaPoll, saaStartDelayElapse, saaPoll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-while-paused", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.Pause}, {Kind: model.UpdateOptions, SetsStartDelay: true}},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-then-restore-original", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{
				{Kind: model.UpdateOptions, SetsStartDelay: true},
				{Kind: model.UpdateOptions, RestoreOriginal: true},
				saaPoll, saaStartDelayElapse, saaPoll,
			},
			startDelayed: true,
		})
	})
}

// TestBackoff_Declarative drives the retry-backoff scenarios (drive only; no assertions yet).
func (s *standaloneActivityTestSuite) TestBackoff_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("backoff/retry-dispatch", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:         []model.Event{saaPoll, saaFailRetryably, saaPoll, saaBackoffDelayElapse, saaPoll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/next-retry-delay-override", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:          []model.Event{saaPoll, saaFailRetryably, saaPoll, saaBackoffDelayElapse, saaPoll},
			maxAttempts:    3,
			nextRetryDelay: saaDelayWindow,
		})
	})
	t.Run("backoff/pause-then-unpause", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:         []model.Event{saaPoll, saaFailRetryably, {Kind: model.Pause}, {Kind: model.Unpause}, saaPoll, saaBackoffDelayElapse, saaPoll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/pause-unpause-then-update", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:         []model.Event{saaPoll, saaFailRetryably, {Kind: model.Pause}, {Kind: model.Unpause}, {Kind: model.UpdateOptions}, saaPoll, saaBackoffDelayElapse, saaPoll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
	t.Run("backoff/next-retry-delay-override-then-update", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:          []model.Event{saaPoll, saaFailRetryably, {Kind: model.UpdateOptions}, saaPoll, saaBackoffDelayElapse, saaPoll},
			maxAttempts:    3,
			nextRetryDelay: saaDelayWindow,
		})
	})
	t.Run("backoff/reset", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:         []model.Event{saaPoll, saaFailRetryably, {Kind: model.Reset}, saaPoll},
			maxAttempts:   3,
			retryInterval: saaDelayWindow,
		})
	})
}

// TestTimeout_Declarative drives the four activity-timeout scenarios (drive only; no assertions yet).
func (s *standaloneActivityTestSuite) TestTimeout_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := s.newTestEnv()
	t := s.T()

	t.Run("schedule-to-close/elapses-while-paused", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{{Kind: model.Pause}, {Kind: model.ScheduleToCloseElapses}},
		})
	})
	t.Run("schedule-to-start/elapses-while-scheduled", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{{Kind: model.ScheduleToStartElapses}},
		})
	})
	t.Run("schedule-to-start/elapses-while-paused", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{{Kind: model.Pause}, {Kind: model.ScheduleToStartElapses}},
		})
	})
	t.Run("start-to-close/elapses-while-started/retries-remain", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{saaPoll, {Kind: model.StartToCloseElapses}},
		})
	})
	t.Run("start-to-close/elapses-while-started/last-attempt", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:       []model.Event{saaPoll, {Kind: model.StartToCloseElapses}},
			maxAttempts: 1,
		})
	})
	t.Run("start-to-close/elapses-while-cancel-requested", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{saaPoll, {Kind: model.RequestCancel}, {Kind: model.StartToCloseElapses}},
		})
	})
	t.Run("heartbeat/elapses-while-started/retries-remain", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace: []model.Event{saaPoll, {Kind: model.HeartbeatElapses}},
		})
	})
	t.Run("heartbeat/elapses-while-started/last-attempt", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:       []model.Event{saaPoll, {Kind: model.HeartbeatElapses}},
			maxAttempts: 1,
		})
	})
	t.Run("schedule-to-start/elapses-within-start-delay", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.ScheduleToStartElapses}},
			startDelayed: true,
		})
	})
	t.Run("schedule-to-close/elapses-within-start-delay", func(t *testing.T) {
		s.runTrace(t, env, saaTrace{
			trace:        []model.Event{{Kind: model.ScheduleToCloseElapses}},
			startDelayed: true,
		})
	})
}

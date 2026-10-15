package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// requestIDsNodePath is the encoded path of the CHASM framework's request IDs data node.
const requestIDsNodePath = "__chasm_requestIDs"

// TestIDConflictPolicy_RequestIDDataNode covers attach-on-conflict request ID dedup with
// history.enableChasmRequestIDDataNode on, where the attach request ID is recorded in the activity's
// request IDs data node instead of on the current record.
func (s *standaloneActivityTestSuite) TestIDConflictPolicy_RequestIDDataNode() {
	env := s.newTestEnv()
	env.OverrideDynamicConfig(dynamicconfig.EnableChasmRequestIDDataNode, true)
	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)
	t := s.T()
	ctx := s.Context()

	conflictRequest := func(activityID, taskQueue, requestID, callbackURL, linkWorkflowID string) *workflowservice.StartActivityExecutionRequest {
		return &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			IdConflictPolicy:    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
			RequestId:           requestID,
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackURL}},
			}},
			Links: []*commonpb.Link{requestIDTestWorkflowEventLink(env.Namespace().String(), linkWorkflowID)},
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
		}
	}

	describe := func(t *testing.T, activityID, runID string) *workflowservice.DescribeActivityExecutionResponse {
		resp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		require.NoError(t, err)
		return resp
	}

	t.Run("RetryAfterCloseReturnsOriginalRun", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		requestID := env.Tv().Any().String()
		ch, callbackAddress := newNexusCompletionHandler(t)

		conflictResp, err := env.FrontendClient().StartActivityExecution(ctx,
			conflictRequest(activityID, taskQueue, requestID, callbackAddress, "original"))
		require.NoError(t, err)
		require.False(t, conflictResp.GetStarted())
		require.Equal(t, startResp.GetRunId(), conflictResp.GetRunId())

		// The attach request ID is recorded in the data node, not on the current record.
		dbState := s.describeMutableState(t, env, activityID).GetDatabaseMutableState()
		require.NotContains(t, dbState.GetExecutionState().GetRequestIds(), requestID)
		requestIDsNode, ok := dbState.GetChasmNodes()[requestIDsNodePath]
		require.True(t, ok)
		requestIDs := &persistencespb.ChasmRequestIDs{}
		require.NoError(t, serialization.Decode(requestIDsNode.GetData(), requestIDs))
		require.Len(t, requestIDs.GetEntries(), 1)
		require.Equal(t, requestID, requestIDs.GetEntries()[0].GetRequestId())

		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, startResp.GetRunId())
		_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.GetTaskToken(),
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		select {
		case <-ch.requestCh:
		case <-ctx.Done():
			require.Fail(t, "timed out waiting for completion callback")
		}
		ch.requestCompleteCh <- nil

		// Retrying the same request after the activity closed returns the original run without
		// starting a new one or attaching the retry's callback and link.
		retryResp, err := env.FrontendClient().StartActivityExecution(ctx,
			conflictRequest(activityID, taskQueue, requestID, "http://localhost/retry-cb", "retry"))
		require.NoError(t, err)
		require.False(t, retryResp.GetStarted())
		require.Equal(t, startResp.GetRunId(), retryResp.GetRunId())

		descResp := describe(t, activityID, startResp.GetRunId())
		require.Len(t, descResp.GetCallbacks(), 1)
		require.Equal(t, callbackAddress, descResp.GetCallbacks()[0].GetInfo().GetCallback().GetNexus().GetUrl())
		require.Len(t, descResp.GetInfo().GetLinks(), 1)
	})

	t.Run("ConcurrentAttachesWithSameRequestIDAttachOnce", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		requestID := env.Tv().Any().String()

		const attempts = 2
		var wg sync.WaitGroup
		responses := make([]*workflowservice.StartActivityExecutionResponse, attempts)
		errs := make([]error, attempts)
		for i := range attempts {
			wg.Go(func() {
				responses[i], errs[i] = env.FrontendClient().StartActivityExecution(ctx,
					conflictRequest(activityID, taskQueue, requestID, "http://localhost/cb", "attempt"))
			})
		}
		wg.Wait()

		for i := range attempts {
			require.NoError(t, errs[i])
			require.False(t, responses[i].GetStarted())
			require.Equal(t, startResp.GetRunId(), responses[i].GetRunId())
		}
		descResp := describe(t, activityID, startResp.GetRunId())
		require.Len(t, descResp.GetCallbacks(), 1, "the same request ID must attach only once")
		require.Len(t, descResp.GetInfo().GetLinks(), 1, "the same request ID must attach only once")
	})
}

func (s *standaloneActivityTestSuite) describeMutableState(
	t *testing.T,
	env *standaloneActivityEnv,
	activityID string,
) *adminservice.DescribeMutableStateResponse {
	resp, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace:   env.Namespace().String(),
		Execution:   &commonpb.WorkflowExecution{WorkflowId: activityID},
		ArchetypeId: activity.ArchetypeID,
	})
	require.NoError(t, err)
	return resp
}

func requestIDTestWorkflowEventLink(namespace, workflowID string) *commonpb.Link {
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      "run-id",
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

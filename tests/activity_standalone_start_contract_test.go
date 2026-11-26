package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestStartContract pins down how StartActivityExecution behaves when the activity ID is already in use,
// for each combination of conflict policy, request ID reuse, on_conflict_options and eager execution.
// These are characterization tests: they describe today's behavior so that changing how the handler
// starts an activity (for example, switching to UpdateWithStartExecution) can be checked against them.
func (s *standaloneActivityTestSuite) TestStartContract() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	attachAll := &commonpb.OnConflictOptions{
		AttachRequestId:           true,
		AttachCompletionCallbacks: true,
		AttachLinks:               true,
	}
	nexusCallback := func(url string) []*commonpb.Callback {
		return []*commonpb.Callback{{
			Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}},
		}}
	}
	workflowEventLink := func(workflowID string) *commonpb.Link {
		return &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
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
	newRequest := func(activityID, taskQueue, requestID string) *workflowservice.StartActivityExecutionRequest {
		return &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            env.Tv().WorkerIdentity(),
			Input:               defaultInput,
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           requestID,
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
	complete := func(t *testing.T, activityID, taskQueue, runID string) {
		pollResp := env.pollActivityTaskAndValidate(ctx, t, activityID, taskQueue, runID)
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Result:    defaultResult,
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
	}
	requireLinkToRun := func(t *testing.T, resp *workflowservice.StartActivityExecutionResponse, activityID, runID string) {
		link := resp.GetLink().GetActivity()
		require.NotNil(t, link)
		require.Equal(t, env.Namespace().String(), link.GetNamespace())
		require.Equal(t, activityID, link.GetActivityId())
		require.Equal(t, runID, link.GetRunId())
	}

	t.Run("Fail_DifferentRequestID_OnConflictAttach_RejectedAndNothingAttached", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		first := env.startAndValidateActivity(ctx, t, activityID, taskQueue)

		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
		req.CompletionCallbacks = nexusCallback("http://localhost/fail-cb")
		req.Links = []*commonpb.Link{workflowEventLink("fail-link")}
		req.OnConflictOptions = attachAll
		_, err := env.FrontendClient().StartActivityExecution(ctx, req)

		var alreadyStarted *serviceerror.ActivityExecutionAlreadyStarted
		require.ErrorAs(t, err, &alreadyStarted)
		require.Equal(t, first.GetRunId(), alreadyStarted.RunId)
		require.Equal(t, env.Tv().RequestID(), alreadyStarted.StartRequestId)

		desc := describe(t, activityID, first.GetRunId())
		require.Empty(t, desc.GetCallbacks())
		require.Empty(t, desc.GetInfo().GetLinks())
	})

	t.Run("Fail_SameRequestID_WithCallbacks_ReturnsExistingRunWithoutDuplicates", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
		req.CompletionCallbacks = nexusCallback("http://localhost/retry-cb")
		req.Links = []*commonpb.Link{workflowEventLink("retry-link")}
		req.OnConflictOptions = attachAll

		first, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.True(t, first.GetStarted())

		retry, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, retry.GetStarted())
		require.Equal(t, first.GetRunId(), retry.GetRunId())
		requireLinkToRun(t, retry, activityID, first.GetRunId())

		desc := describe(t, activityID, first.GetRunId())
		require.Len(t, desc.GetCallbacks(), 1)
		require.Len(t, desc.GetInfo().GetLinks(), 1)
	})

	t.Run("UseExisting_SameRequestID_ReturnsExistingRunWithLink", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING

		first, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.True(t, first.GetStarted())

		retry, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, retry.GetStarted())
		require.Equal(t, first.GetRunId(), retry.GetRunId())
		requireLinkToRun(t, retry, activityID, first.GetRunId())
	})

	t.Run("UseExisting_NoOnConflictOptions_DoesNotAttach", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		first := env.startAndValidateActivity(ctx, t, activityID, taskQueue)

		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
		req.CompletionCallbacks = nexusCallback("http://localhost/dropped-cb")
		req.Links = []*commonpb.Link{workflowEventLink("dropped-link")}
		resp, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, resp.GetStarted())
		require.Equal(t, first.GetRunId(), resp.GetRunId())

		desc := describe(t, activityID, first.GetRunId())
		require.Empty(t, desc.GetCallbacks())
		require.Empty(t, desc.GetInfo().GetLinks())
	})

	t.Run("UseExisting_OnConflictAttachLinksOnly_DoesNotAttachCallbacks", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		first := env.startAndValidateActivity(ctx, t, activityID, taskQueue)

		links := []*commonpb.Link{workflowEventLink("links-only")}
		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
		req.CompletionCallbacks = nexusCallback("http://localhost/not-attached-cb")
		req.Links = links
		req.OnConflictOptions = &commonpb.OnConflictOptions{AttachLinks: true}
		resp, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, resp.GetStarted())

		desc := describe(t, activityID, first.GetRunId())
		require.Empty(t, desc.GetCallbacks())
		require.Len(t, desc.GetInfo().GetLinks(), 1)
	})

	t.Run("UseExisting_EagerRequestedOnExisting_NoEagerTask", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		first := env.startAndValidateActivity(ctx, t, activityID, taskQueue)

		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
		req.RequestEagerExecution = true
		resp, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, resp.GetStarted())
		require.Equal(t, first.GetRunId(), resp.GetRunId())
		require.Nil(t, resp.GetEagerActivityTask())
	})

	t.Run("Closed_SameRequestIDAsOriginal_ReturnsOriginalRun", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		requestID := env.Tv().Any().String()
		req := newRequest(activityID, taskQueue, requestID)

		first, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.True(t, first.GetStarted())
		complete(t, activityID, taskQueue, first.GetRunId())

		retry, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.False(t, retry.GetStarted())
		require.Equal(t, first.GetRunId(), retry.GetRunId())
	})

	t.Run("Closed_DifferentRequestID_AllowDuplicate_StartsNewRun", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		first := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		complete(t, activityID, taskQueue, first.GetRunId())

		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
		req.CompletionCallbacks = nexusCallback("http://localhost/new-run-cb")
		req.OnConflictOptions = attachAll
		resp, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.GetStarted())
		require.NotEqual(t, first.GetRunId(), resp.GetRunId())
		requireLinkToRun(t, resp, activityID, resp.GetRunId())

		// Callbacks on the request belong to the new run, not the closed one.
		require.Len(t, describe(t, activityID, resp.GetRunId()).GetCallbacks(), 1)
		require.Empty(t, describe(t, activityID, first.GetRunId()).GetCallbacks())
	})

	t.Run("NotFound_WithCallbacksAndLinks_AttachesOnce", func(t *testing.T) {
		activityID := testcore.RandomizeStr(t.Name())
		taskQueue := testcore.RandomizeStr(t.Name())
		req := newRequest(activityID, taskQueue, env.Tv().Any().String())
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
		req.CompletionCallbacks = nexusCallback("http://localhost/create-cb")
		req.Links = []*commonpb.Link{workflowEventLink("create-link")}
		req.OnConflictOptions = attachAll
		resp, err := env.FrontendClient().StartActivityExecution(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.GetStarted())

		desc := describe(t, activityID, resp.GetRunId())
		require.Len(t, desc.GetCallbacks(), 1)
		require.Len(t, desc.GetInfo().GetLinks(), 1)
	})
}

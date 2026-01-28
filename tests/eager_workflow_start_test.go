package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestEagerWorkflow(t *testing.T) {
	t.Run("StartNew", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		respondComplete := func(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
			respondWorkflowTaskCompletedHelper(t, s.FrontendClient(), s.Namespace().String(), task, result)
		}
		getResult := func(runID string) string {
			return getWorkflowStringResultHelper(t, s.FrontendGRPCAddress(), s.Namespace().String(), workflowID, runID)
		}

		// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
		// response.
		response := startEager(&workflowservice.StartWorkflowExecutionRequest{
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomKeywordField": {
						Metadata: map[string][]byte{"encoding": []byte("json/plain")},
						Data:     []byte(`"value"`),
					},
				},
			},
		})
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")
		startedEventAttrs := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
		require.True(t, startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
		kwData := startedEventAttrs.SearchAttributes.IndexedFields["CustomKeywordField"].Data
		require.Equal(t, `"value"`, string(kwData))
		respondComplete(task, "ok")
		// Verify workflow completes and client can get the result
		result := getResult(response.RunId)
		require.Equal(t, "ok", result)
	})

	t.Run("RetryTaskAfterTimeout", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		respondComplete := func(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
			respondWorkflowTaskCompletedHelper(t, s.FrontendClient(), s.Namespace().String(), task, result)
		}
		pollTask := func() *workflowservice.PollWorkflowTaskQueueResponse {
			return pollWorkflowTaskQueueHelper(t, s.FrontendClient(), s.Namespace().String(), taskQueue)
		}
		getResult := func(runID string) string {
			return getWorkflowStringResultHelper(t, s.FrontendGRPCAddress(), s.Namespace().String(), workflowID, runID)
		}

		response := startEager(&workflowservice.StartWorkflowExecutionRequest{
			// Should give enough grace time even in slow CI
			WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		})
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")
		// Let it timeout so it can be polled via standard matching based dispatch
		task = pollTask()
		respondComplete(task, "ok")
		// Verify workflow completes and client can get the result
		result := getResult(response.RunId)
		require.Equal(t, "ok", result)
	})

	t.Run("RetryStartAfterTimeout", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		respondComplete := func(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
			respondWorkflowTaskCompletedHelper(t, s.FrontendClient(), s.Namespace().String(), task, result)
		}
		pollTask := func() *workflowservice.PollWorkflowTaskQueueResponse {
			return pollWorkflowTaskQueueHelper(t, s.FrontendClient(), s.Namespace().String(), taskQueue)
		}
		getResult := func(runID string) string {
			return getWorkflowStringResultHelper(t, s.FrontendGRPCAddress(), s.Namespace().String(), workflowID, runID)
		}

		request := &workflowservice.StartWorkflowExecutionRequest{
			// Should give enough grace time even in slow CI
			WorkflowTaskTimeout: durationpb.New(2 * time.Second),
			RequestId:           uuid.NewString(),
		}
		response := startEager(request)
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")

		// Let it timeout
		time.Sleep(request.WorkflowTaskTimeout.AsDuration()) //nolint:forbidigo
		response = startEager(request)
		task = response.GetEagerWorkflowTask()
		require.Nil(t, task, "StartWorkflowExecution response contained a workflow task")

		task = pollTask()
		respondComplete(task, "ok")
		// Verify workflow completes and client can get the result
		result := getResult(response.RunId)
		require.Equal(t, "ok", result)
	})

	t.Run("RetryStartImmediately", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		respondComplete := func(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
			respondWorkflowTaskCompletedHelper(t, s.FrontendClient(), s.Namespace().String(), task, result)
		}
		getResult := func(runID string) string {
			return getWorkflowStringResultHelper(t, s.FrontendGRPCAddress(), s.Namespace().String(), workflowID, runID)
		}

		request := &workflowservice.StartWorkflowExecutionRequest{RequestId: uuid.NewString()}
		response := startEager(request)
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")
		response = startEager(request)
		task = response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")

		respondComplete(task, "ok")
		// Verify workflow completes and client can get the result
		result := getResult(response.RunId)
		require.Equal(t, "ok", result)
	})

	t.Run("TerminateDuplicate", func(t *testing.T) {
		s := testcore.NewEnv(t)

		// reset reuse minimal interval to allow workflow termination
		s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		respondComplete := func(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
			respondWorkflowTaskCompletedHelper(t, s.FrontendClient(), s.Namespace().String(), task, result)
		}
		getResult := func(runID string) string {
			return getWorkflowStringResultHelper(t, s.FrontendGRPCAddress(), s.Namespace().String(), workflowID, runID)
		}

		request := &workflowservice.StartWorkflowExecutionRequest{
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
		}
		startEager(request)
		response := startEager(request)
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")

		respondComplete(task, "ok")
		// Verify workflow completes and client can get the result
		result := getResult(response.RunId)
		require.Equal(t, "ok", result)
	})

	t.Run("WorkflowRetry", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		taskQueue := s.Tv().TaskQueue()

		startEager := func(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
			return startEagerWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), workflowID, taskQueue, baseOptions)
		}
		failWf := func(task *workflowservice.PollWorkflowTaskQueueResponse, msg string) {
			failWorkflowHelper(t, s.FrontendClient(), s.Namespace().String(), task, msg)
		}
		pollTask := func() *workflowservice.PollWorkflowTaskQueueResponse {
			return pollWorkflowTaskQueueHelper(t, s.FrontendClient(), s.Namespace().String(), taskQueue)
		}

		// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
		// response.
		response := startEager(&workflowservice.StartWorkflowExecutionRequest{
			RequestId: uuid.NewString(),
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					"CustomKeywordField": {
						Metadata: map[string][]byte{"encoding": []byte("json/plain")},
						Data:     []byte(`"value"`),
					},
				},
			},
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 2,
			},
		})
		task := response.GetEagerWorkflowTask()
		require.NotNil(t, task, "StartWorkflowExecution response did not contain a workflow task")
		startedEventAttrs := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
		require.True(t, startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
		kwData := startedEventAttrs.SearchAttributes.IndexedFields["CustomKeywordField"].GetData()
		require.Equal(t, `"value"`, string(kwData))

		// fail workflow
		failWf(task, "failure 1")
		// fail retry workflow
		task = pollTask()
		failWf(task, "failure 2")

		s.EventuallyWithT(
			func(c *assert.CollectT) {
				resp, err := s.FrontendClient().CountWorkflowExecutions(
					testcore.NewContext(),
					&workflowservice.CountWorkflowExecutionsRequest{
						Namespace: s.Namespace().String(),
						Query:     fmt.Sprintf("WorkflowId = '%s' AND ExecutionStatus = 'Failed'", workflowID),
					},
				)
				require.NoError(c, err)
				require.Equal(c, int64(2), resp.Count)
			},
			testcore.WaitForESToSettle,
			200*time.Millisecond,
		)
	})
}

func startEagerWorkflowHelper(t *testing.T, frontendClient workflowservice.WorkflowServiceClient, namespace, workflowID string, taskQueue *taskqueuepb.TaskQueue, baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
	options := proto.Clone(baseOptions).(*workflowservice.StartWorkflowExecutionRequest) //nolint:revive
	options.RequestEagerExecution = true

	if options.GetNamespace() == "" {
		options.Namespace = namespace
	}
	if options.Identity == "" {
		options.Identity = "test"
	}
	if options.WorkflowId == "" {
		options.WorkflowId = workflowID
	}
	if options.WorkflowType == nil {
		options.WorkflowType = &commonpb.WorkflowType{Name: "Workflow"}
	}
	if options.TaskQueue == nil {
		options.TaskQueue = taskQueue
	}
	if options.RequestId == "" {
		options.RequestId = uuid.NewString()
	}

	response, err := frontendClient.StartWorkflowExecution(testcore.NewContext(), options)
	require.NoError(t, err)

	return response
}

func respondWorkflowTaskCompletedHelper(t *testing.T, frontendClient workflowservice.WorkflowServiceClient, namespace string, task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
	dataConverter := converter.GetDefaultDataConverter()
	payloads, err := dataConverter.ToPayloads(result)
	require.NoError(t, err)
	completion := workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: namespace,
		Identity:  "test",
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads,
			},
		}}},
	}
	_, err = frontendClient.RespondWorkflowTaskCompleted(testcore.NewContext(), &completion)
	require.NoError(t, err)
}

func failWorkflowHelper(t *testing.T, frontendClient workflowservice.WorkflowServiceClient, namespace string, task *workflowservice.PollWorkflowTaskQueueResponse, msg string) {
	completion := workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: namespace,
		Identity:  "test",
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
				FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: &failurepb.Failure{
						Message: msg,
					},
				},
			},
		}},
	}
	_, err := frontendClient.RespondWorkflowTaskCompleted(testcore.NewContext(), &completion)
	require.NoError(t, err)
}

func pollWorkflowTaskQueueHelper(t *testing.T, frontendClient workflowservice.WorkflowServiceClient, namespace string, taskQueue *taskqueuepb.TaskQueue) *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := frontendClient.PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: taskQueue,
		Identity:  "test",
	})
	require.NotNil(t, task, "PollWorkflowTaskQueue response was empty")
	require.NoError(t, err)
	return task
}

func getWorkflowStringResultHelper(t *testing.T, hostPort, namespace, workflowID, runID string) string {
	c, err := client.Dial(client.Options{HostPort: hostPort, Namespace: namespace})
	require.NoError(t, err)
	run := c.GetWorkflow(testcore.NewContext(), workflowID, runID)
	var result string
	err = run.Get(testcore.NewContext(), &result)
	require.NoError(t, err)
	return result
}

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/protohelpers/match"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type EagerWorkflowTestSuite struct {
	parallelsuite.Suite[*EagerWorkflowTestSuite]
}

func TestEagerWorkflowTestSuite(t *testing.T) {
	parallelsuite.Run(t, &EagerWorkflowTestSuite{})
}

func (s *EagerWorkflowTestSuite) startEagerWorkflow(env *testcore.TestEnv, baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
	options := env.Requests().StartWorkflowExecution(baseOptions)
	options.RequestEagerExecution = true

	response, err := env.FrontendClient().StartWorkflowExecution(s.Context(), options)
	s.NoError(err)

	return response
}

func (s *EagerWorkflowTestSuite) respondWorkflowTaskCompleted(env *testcore.TestEnv, task *workflowservice.PollWorkflowTaskQueueResponse, result any) {
	dataConverter := converter.GetDefaultDataConverter()
	payloads, err := dataConverter.ToPayloads(result)
	s.NoError(err)
	completion := env.Requests().RespondWorkflowTaskCompleted(&workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads,
			},
		}}},
	})
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), completion)
	s.NoError(err)
}

func (s *EagerWorkflowTestSuite) failWorkflow(env *testcore.TestEnv, task *workflowservice.PollWorkflowTaskQueueResponse, msg string) {
	completion := env.Requests().RespondWorkflowTaskCompleted(&workflowservice.RespondWorkflowTaskCompletedRequest{
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
	})
	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), completion)
	s.NoError(err)
}

func (s *EagerWorkflowTestSuite) pollWorkflowTaskQueue(env *testcore.TestEnv) *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), env.Requests().PollWorkflowTaskQueue(&workflowservice.PollWorkflowTaskQueueRequest{}))
	s.NotNil(task, "PollWorkflowTaskQueue response was empty")
	s.NoError(err)
	return task
}

func (s *EagerWorkflowTestSuite) getWorkflowStringResult(env *testcore.TestEnv, workflowID, runID string) string {
	c, err := client.Dial(client.Options{HostPort: env.FrontendGRPCAddress(), Namespace: env.Namespace().String()})
	s.NoError(err)
	run := c.GetWorkflow(s.Context(), workflowID, runID)
	var result string
	err = run.Get(s.Context(), &result)
	s.NoError(err)
	return result
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_StartNew() {
	env := testcore.NewEnv(s.T())
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(env, &workflowservice.StartWorkflowExecutionRequest{
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": {
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"value"`),
				},
			},
		},
	})
	// Assert the whole response shape exhaustively: every field must be
	// accounted for, so a new response field would force this to be revisited.
	// The eager task is a complex nested message, so match.Custom inspects it:
	// eager execution must be accepted and the per-namespace search attribute
	// mapping must have been applied in the response.
	match.StartWorkflowExecutionResponse{
		RunId:   match.NotEmpty(),
		Started: true,
		EagerWorkflowTask: match.Custom(func(task *workflowservice.PollWorkflowTaskQueueResponse) error {
			if task == nil {
				return fmt.Errorf("expected an eager workflow task")
			}
			attrs := task.GetHistory().GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			if !attrs.GetEagerExecutionAccepted() {
				return fmt.Errorf("eager execution should be accepted")
			}
			if got := string(attrs.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"].GetData()); got != `"value"` {
				return fmt.Errorf("CustomKeywordField = %s, want %q", got, `"value"`)
			}
			return nil
		}),
		Status:              match.Any(),
		Link:                match.Any(),
		FirstExecutionRunId: match.Any(),
	}.Equal(s.T(), response)

	s.respondWorkflowTaskCompleted(env, response.GetEagerWorkflowTask(), "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(env, env.Tv().WorkflowID(), response.RunId)
	s.Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryTaskAfterTimeout() {
	env := testcore.NewEnv(s.T())
	response := s.startEagerWorkflow(env, &workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
	})
	task := response.GetEagerWorkflowTask()
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	// Let it timeout so it can be polled via standard matching based dispatch
	task = s.pollWorkflowTaskQueue(env)
	s.respondWorkflowTaskCompleted(env, task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(env, env.Tv().WorkflowID(), response.RunId)
	s.Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartAfterTimeout() {
	env := testcore.NewEnv(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		RequestId:           uuid.NewString(),
	}
	response := s.startEagerWorkflow(env, request)
	task := response.GetEagerWorkflowTask()
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	// Let it timeout
	time.Sleep(request.WorkflowTaskTimeout.AsDuration()) //nolint:forbidigo
	response = s.startEagerWorkflow(env, request)
	task = response.GetEagerWorkflowTask()
	s.Nil(task, "StartWorkflowExecution response contained a workflow task")

	task = s.pollWorkflowTaskQueue(env)
	s.respondWorkflowTaskCompleted(env, task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(env, env.Tv().WorkflowID(), response.RunId)
	s.Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartImmediately() {
	env := testcore.NewEnv(s.T())
	request := &workflowservice.StartWorkflowExecutionRequest{RequestId: uuid.NewString()}
	response := s.startEagerWorkflow(env, request)
	task := response.GetEagerWorkflowTask()
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	response = s.startEagerWorkflow(env, request)
	task = response.GetEagerWorkflowTask()
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(env, task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(env, env.Tv().WorkflowID(), response.RunId)
	s.Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_TerminateDuplicate() {
	// reset reuse minimal interval to allow workflow termination
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0))

	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}
	s.startEagerWorkflow(env, request)
	response := s.startEagerWorkflow(env, request)
	task := response.GetEagerWorkflowTask()
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(env, task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(env, env.Tv().WorkflowID(), response.RunId)
	s.Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_WorkflowRetry() {
	env := testcore.NewEnv(s.T())
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(env, &workflowservice.StartWorkflowExecutionRequest{
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
	s.NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	startedEventAttrs := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.True(startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
	kwData := startedEventAttrs.SearchAttributes.IndexedFields["CustomKeywordField"].GetData()
	s.Equal(`"value"`, string(kwData))

	// fail workflow
	s.failWorkflow(env, task, "failure 1")
	// fail retry workflow
	task = s.pollWorkflowTaskQueue(env)
	s.failWorkflow(env, task, "failure 2")

	s.Await(func(s *EagerWorkflowTestSuite) {
		resp, err := env.FrontendClient().CountWorkflowExecutions(
			s.Context(),
			&workflowservice.CountWorkflowExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("WorkflowId = '%s' AND ExecutionStatus = 'Failed'", env.Tv().WorkflowID()),
			},
		)
		s.NoError(err)
		s.Equal(int64(2), resp.Count)
	}, testcore.WaitForESToSettle, 200*time.Millisecond)
}

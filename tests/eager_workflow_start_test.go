package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

type EagerWorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestEagerWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(EagerWorkflowTestSuite))
}

func (s *EagerWorkflowTestSuite) defaultWorkflowID() string {
	return fmt.Sprintf("functional-%v", s.T().Name())
}

func (s *EagerWorkflowTestSuite) defaultTaskQueue() *taskqueuepb.TaskQueue {
	name := fmt.Sprintf("functional-queue-%v", s.T().Name())
	return &taskqueuepb.TaskQueue{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func (s *EagerWorkflowTestSuite) startEagerWorkflow(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
	options := proto.Clone(baseOptions).(*workflowservice.StartWorkflowExecutionRequest) //nolint:revive
	options.RequestEagerExecution = true

	if options.GetNamespace() == "" {
		options.Namespace = s.Namespace().String()
	}
	if options.Identity == "" {
		options.Identity = "test"
	}
	if options.WorkflowId == "" {
		options.WorkflowId = s.defaultWorkflowID()
	}
	if options.WorkflowType == nil {
		options.WorkflowType = &commonpb.WorkflowType{Name: "Workflow"}
	}
	if options.TaskQueue == nil {
		options.TaskQueue = s.defaultTaskQueue()
	}
	if options.RequestId == "" {
		options.RequestId = uuid.NewString()
	}

	response, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), options)
	s.Require().NoError(err)

	return response
}

func (s *EagerWorkflowTestSuite) respondWorkflowTaskCompleted(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
	dataConverter := converter.GetDefaultDataConverter()
	payloads, err := dataConverter.ToPayloads(result)
	s.Require().NoError(err)
	completion := workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test",
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads,
			},
		}}},
	}
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &completion)
	s.Require().NoError(err)
}

func (s *EagerWorkflowTestSuite) failWorkflow(task *workflowservice.PollWorkflowTaskQueueResponse, msg string) {
	completion := workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
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
	_, err := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &completion)
	s.Require().NoError(err)
}

func (s *EagerWorkflowTestSuite) pollWorkflowTaskQueue() *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: s.defaultTaskQueue(),
		Identity:  "test",
	})
	s.Require().NotNil(task, "PollWorkflowTaskQueue response was empty")
	s.Require().NoError(err)
	return task
}

func (s *EagerWorkflowTestSuite) getWorkflowStringResult(workflowID, runID string) string {
	c, err := client.Dial(client.Options{HostPort: s.FrontendGRPCAddress(), Namespace: s.Namespace().String()})
	s.Require().NoError(err)
	run := c.GetWorkflow(testcore.NewContext(), workflowID, runID)
	var result string
	err = run.Get(testcore.NewContext(), &result)
	s.Require().NoError(err)
	return result
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_StartNew() {
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(&workflowservice.StartWorkflowExecutionRequest{
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
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	startedEventAttrs := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.Require().True(startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
	kwData := startedEventAttrs.SearchAttributes.IndexedFields["CustomKeywordField"].Data
	s.Require().Equal(`"value"`, string(kwData))
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryTaskAfterTimeout() {
	response := s.startEagerWorkflow(&workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
	})
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	// Let it timeout so it can be polled via standard matching based dispatch
	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartAfterTimeout() {
	request := &workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		RequestId:           uuid.NewString(),
	}
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	// Let it timeout
	time.Sleep(request.WorkflowTaskTimeout.AsDuration()) //nolint:forbidigo
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().Nil(task, "StartWorkflowExecution response contained a workflow task")

	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartImmediately() {
	request := &workflowservice.StartWorkflowExecutionRequest{RequestId: uuid.NewString()}
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_TerminateDuplicate() {

	// reset reuse minimal interval to allow workflow termination
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}
	s.startEagerWorkflow(request)
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_WorkflowRetry() {
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(&workflowservice.StartWorkflowExecutionRequest{
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
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	startedEventAttrs := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.Require().True(startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
	kwData := startedEventAttrs.SearchAttributes.IndexedFields["CustomKeywordField"].GetData()
	s.Require().Equal(`"value"`, string(kwData))

	// fail workflow
	s.failWorkflow(task, "failure 1")
	// fail retry workflow
	task = s.pollWorkflowTaskQueue()
	s.failWorkflow(task, "failure 2")

	s.Require().EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.FrontendClient().CountWorkflowExecutions(
				testcore.NewContext(),
				&workflowservice.CountWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("WorkflowId = '%s' AND ExecutionStatus = 'Failed'", s.defaultWorkflowID()),
				},
			)
			require.NoError(c, err)
			require.Equal(c, int64(2), resp.Count)
		},
		testcore.WaitForESToSettle,
		200*time.Millisecond,
	)
}

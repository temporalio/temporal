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
	return taskqueuepb.TaskQueue_builder{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
}

func (s *EagerWorkflowTestSuite) startEagerWorkflow(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
	options := proto.Clone(baseOptions).(*workflowservice.StartWorkflowExecutionRequest) //nolint:revive
	options.SetRequestEagerExecution(true)

	if options.GetNamespace() == "" {
		options.SetNamespace(s.Namespace().String())
	}
	if options.GetIdentity() == "" {
		options.SetIdentity("test")
	}
	if options.GetWorkflowId() == "" {
		options.SetWorkflowId(s.defaultWorkflowID())
	}
	if !options.HasWorkflowType() {
		options.SetWorkflowType(commonpb.WorkflowType_builder{Name: "Workflow"}.Build())
	}
	if !options.HasTaskQueue() {
		options.SetTaskQueue(s.defaultTaskQueue())
	}
	if options.GetRequestId() == "" {
		options.SetRequestId(uuid.NewString())
	}

	response, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), options)
	s.Require().NoError(err)

	return response
}

func (s *EagerWorkflowTestSuite) respondWorkflowTaskCompleted(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
	dataConverter := converter.GetDefaultDataConverter()
	payloads, err := dataConverter.ToPayloads(result)
	s.Require().NoError(err)
	completion := workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		Identity:  "test",
		TaskToken: task.GetTaskToken(),
		Commands: []*commandpb.Command{commandpb.Command_builder{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
			Result: payloads,
		}.Build()}.Build()},
	}.Build()
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), completion)
	s.Require().NoError(err)
}

func (s *EagerWorkflowTestSuite) failWorkflow(task *workflowservice.PollWorkflowTaskQueueResponse, msg string) {
	completion := workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		Identity:  "test",
		TaskToken: task.GetTaskToken(),
		Commands: []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			FailWorkflowExecutionCommandAttributes: commandpb.FailWorkflowExecutionCommandAttributes_builder{
				Failure: failurepb.Failure_builder{
					Message: msg,
				}.Build(),
			}.Build(),
		}.Build()},
	}.Build()
	_, err := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), completion)
	s.Require().NoError(err)
}

func (s *EagerWorkflowTestSuite) pollWorkflowTaskQueue() *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: s.defaultTaskQueue(),
		Identity:  "test",
	}.Build())
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
	response := s.startEagerWorkflow(workflowservice.StartWorkflowExecutionRequest_builder{
		SearchAttributes: commonpb.SearchAttributes_builder{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": commonpb.Payload_builder{
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"value"`),
				}.Build(),
			},
		}.Build(),
	}.Build())
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	startedEventAttrs := task.GetHistory().GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	s.Require().True(startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
	kwData := startedEventAttrs.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"].GetData()
	s.Require().Equal(`"value"`, string(kwData))
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.GetRunId())
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryTaskAfterTimeout() {
	response := s.startEagerWorkflow(workflowservice.StartWorkflowExecutionRequest_builder{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
	}.Build())
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	// Let it timeout so it can be polled via standard matching based dispatch
	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.GetRunId())
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartAfterTimeout() {
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		RequestId:           uuid.NewString(),
	}.Build()
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	// Let it timeout
	time.Sleep(request.GetWorkflowTaskTimeout().AsDuration()) //nolint:forbidigo
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().Nil(task, "StartWorkflowExecution response contained a workflow task")

	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.GetRunId())
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_RetryStartImmediately() {
	request := workflowservice.StartWorkflowExecutionRequest_builder{RequestId: uuid.NewString()}.Build()
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.GetRunId())
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_TerminateDuplicate() {

	// reset reuse minimal interval to allow workflow termination
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}.Build()
	s.startEagerWorkflow(request)
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.GetRunId())
	s.Require().Equal("ok", result)
}

func (s *EagerWorkflowTestSuite) TestEagerWorkflowStart_WorkflowRetry() {
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId: uuid.NewString(),
		SearchAttributes: commonpb.SearchAttributes_builder{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": commonpb.Payload_builder{
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"value"`),
				}.Build(),
			},
		}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			MaximumAttempts: 2,
		}.Build(),
	}.Build())
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	startedEventAttrs := task.GetHistory().GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	s.Require().True(startedEventAttrs.GetEagerExecutionAccepted(), "Eager execution should be accepted")
	kwData := startedEventAttrs.GetSearchAttributes().GetIndexedFields()["CustomKeywordField"].GetData()
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
				workflowservice.CountWorkflowExecutionsRequest_builder{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("WorkflowId = '%s' AND ExecutionStatus = 'Failed'", s.defaultWorkflowID()),
				}.Build(),
			)
			require.NoError(c, err)
			require.Equal(c, int64(2), resp.GetCount())
		},
		testcore.WaitForESToSettle,
		200*time.Millisecond,
	)
}

package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NamespaceInterceptorTestSuite struct {
	testcore.FunctionalTestBase
}

func TestNamespaceInterceptorTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NamespaceInterceptorTestSuite))
}

func (s *NamespaceInterceptorTestSuite) TestServerRejectsInvalidRequests() {
	sut := newSystemUnderTestConnector(s)

	customersNamespace := s.Namespace()
	err := sut.startWorkflowExecution(customersNamespace)
	s.NoError(err)

	task, err := sut.pollWorkflowTaskQueue(customersNamespace)
	s.NoError(err)

	// customer tries to use another namespace
	err = sut.respondWorkflowTaskCompleted(task, namespace.Name("another-namespace"))
	s.Error(err, "Invalid request was processed successfully, make sure NamespaceRequestValidator interceptor is used")

	err = sut.respondWorkflowTaskCompleted(task, customersNamespace)
	s.NoError(err, "Valid request was rejected")
}

type sutConnector struct {
	suite           *NamespaceInterceptorTestSuite
	identity        string
	taskQueue       *taskqueuepb.TaskQueue
	stickyTaskQueue *taskqueuepb.TaskQueue
	id              string
	taskToken       []byte
}

func newSystemUnderTestConnector(s *NamespaceInterceptorTestSuite) *sutConnector {
	id := uuid.NewString()
	return &sutConnector{
		suite:           s,
		identity:        "worker-1",
		taskQueue:       &taskqueuepb.TaskQueue{Name: id, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		stickyTaskQueue: &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: id},
		id:              id,
	}

}

func (b *sutConnector) startWorkflowExecution(ns namespace.Name) error {
	request := newStartWorkflowExecutionRequest(ns, b.id, b.identity, b.taskQueue)

	_, err := b.suite.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	return err
}

func (b *sutConnector) pollWorkflowTaskQueue(ns namespace.Name) ([]byte, error) {
	request := newPollWorkflowTaskQueueRequest(ns, b.identity, b.taskQueue)
	resp, err := b.suite.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), request)
	if err != nil {
		return nil, err
	}
	b.taskToken = resp.GetTaskToken()
	return b.taskToken, nil
}

func (b *sutConnector) respondWorkflowTaskCompleted(token []byte, ns namespace.Name) error {
	request := newRespondWorkflowTaskCompletedRequest(ns, b.stickyTaskQueue, token)
	_, err := b.suite.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), request)
	return err
}

func newStartWorkflowExecutionRequest(ns namespace.Name, workflowId string, identity string, queue *taskqueuepb.TaskQueue) *workflowservice.StartWorkflowExecutionRequest {
	wt := "functional-workflow-namespace-validator-interceptor"
	workflowType := &commonpb.WorkflowType{Name: wt}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns.String(),
		WorkflowId:          workflowId,
		WorkflowType:        workflowType,
		TaskQueue:           queue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}
	return request
}

func newPollWorkflowTaskQueueRequest(ns namespace.Name, identity string, queue *taskqueuepb.TaskQueue) *workflowservice.PollWorkflowTaskQueueRequest {
	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns.String(),
		TaskQueue: queue,
		Identity:  identity,
	}
}

func newRespondWorkflowTaskCompletedRequest(ns namespace.Name, stickyQueue *taskqueuepb.TaskQueue, token []byte) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: ns.String(),
		TaskToken: token,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("efg"),
					},
				},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	}
}

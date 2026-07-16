package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NamespaceInterceptorTestSuite struct {
	parallelsuite.Suite[*NamespaceInterceptorTestSuite]
}

func TestNamespaceInterceptorTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NamespaceInterceptorTestSuite{})
}

func (s *NamespaceInterceptorTestSuite) TestNamespaceInterceptorServerRejectsInvalidRequests() {
	env := testcore.NewEnv(s.T())
	id := uuid.NewString()
	sut := &sutConnector{
		env:             env,
		identity:        "worker-1",
		taskQueue:       &taskqueuepb.TaskQueue{Name: id, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		stickyTaskQueue: &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: id},
		id:              id,
	}

	customersNamespace := env.Namespace()
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
	env             *testcore.TestEnv
	identity        string
	taskQueue       *taskqueuepb.TaskQueue
	stickyTaskQueue *taskqueuepb.TaskQueue
	id              string
	taskToken       []byte
}

func (b *sutConnector) startWorkflowExecution(ns namespace.Name) error {
	_, err := b.env.FrontendClient().StartWorkflowExecution(b.env.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns.String(),
		WorkflowId:          b.id,
		WorkflowType:        &commonpb.WorkflowType{Name: "functional-workflow-namespace-validator-interceptor"},
		TaskQueue:           b.taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            b.identity,
	})
	return err
}

func (b *sutConnector) pollWorkflowTaskQueue(ns namespace.Name) ([]byte, error) {
	resp, err := b.env.FrontendClient().PollWorkflowTaskQueue(b.env.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns.String(),
		TaskQueue: b.taskQueue,
		Identity:  b.identity,
	})
	if err != nil {
		return nil, err
	}
	b.taskToken = resp.GetTaskToken()
	return b.taskToken, nil
}

func (b *sutConnector) respondWorkflowTaskCompleted(token []byte, ns namespace.Name) error {
	_, err := b.env.FrontendClient().RespondWorkflowTaskCompleted(b.env.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
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
			WorkerTaskQueue:        b.stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	})
	return err
}

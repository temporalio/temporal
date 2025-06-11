package tests

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/tests/testcore"
)

var (
	ErrInvalidRunCount = errors.New("invalid run count")
)

type ClientDataConverterTestSuite struct {
	testcore.FunctionalTestBase
}

func TestClientDataConverterTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ClientDataConverterTestSuite))
}

func testActivity(_ workflow.Context, msg string) (string, error) {
	return "hello_" + msg, nil
}

func testDataConverterWorkflow(ctx workflow.Context, tl string) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 20 * time.Second,
		StartToCloseTimeout:    40 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, testActivity, "world").Get(ctx, &result)
	if err != nil {
		return "", err
	}

	// use another converter to run activity,
	// with new taskQueue so that worker with same data converter can properly process tasks.
	var result1 string
	ctx1 := workflow.WithDataConverter(ctx, testcore.NewTestDataConverter())
	ctx1 = workflow.WithTaskQueue(ctx1, tl)
	err1 := workflow.ExecuteActivity(ctx1, testActivity, "world1").Get(ctx1, &result1)
	if err1 != nil {
		return "", err1
	}
	return result + "," + result1, nil
}

func testChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow execution started")
	if runCount <= 0 {
		logger.Error("Invalid valid for run count", "RunCount", runCount)
		return "", ErrInvalidRunCount
	}

	totalCount++
	runCount--
	if runCount == 0 {
		result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
		logger.Info("Child workflow completed", "Result", result)
		return strconv.Itoa(totalCount), nil
	}

	logger.Info("Child workflow starting new run", "RunCount", runCount, "TotalCount", totalCount)
	return "", workflow.NewContinueAsNewError(ctx, testChildWorkflow, totalCount, runCount)
}

func (s *ClientDataConverterTestSuite) startWorkerWithDataConverter(tl string, dataConverter converter.DataConverter) (sdkclient.Client, worker.Worker) {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:      s.FrontendGRPCAddress(),
		Namespace:     s.Namespace().String(),
		DataConverter: dataConverter,
	})
	s.NoError(err)

	newWorker := worker.New(sdkClient, tl, worker.Options{})
	newWorker.RegisterActivity(testActivity)
	newWorker.RegisterWorkflow(testChildWorkflow)

	err = newWorker.Start()
	s.NoError(err)
	return sdkClient, newWorker
}

var childTaskQueue = "client-func-data-converter-child-taskqueue"

func testParentWorkflow(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	execution := workflow.GetInfo(ctx).WorkflowExecution
	childID := fmt.Sprintf("child_workflow:%v", execution.RunID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:         childID,
		WorkflowRunTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, testChildWorkflow, 0, 3).Get(ctx, &result)
	if err != nil {
		logger.Error("Parent execution received child execution failure", "error", err)
		return "", err
	}

	childID1 := fmt.Sprintf("child_workflow1:%v", execution.RunID)
	cwo1 := workflow.ChildWorkflowOptions{
		WorkflowID:         childID1,
		WorkflowRunTimeout: time.Minute,
		TaskQueue:          childTaskQueue,
	}
	ctx1 := workflow.WithChildOptions(ctx, cwo1)
	ctx1 = workflow.WithDataConverter(ctx1, testcore.NewTestDataConverter())
	var result1 string
	err1 := workflow.ExecuteChildWorkflow(ctx1, testChildWorkflow, 0, 2).Get(ctx1, &result1)
	if err1 != nil {
		logger.Error("Parent execution received child execution 1 failure", "error", err1)
		return "", err1
	}

	res := fmt.Sprintf("Complete child1 %s times, complete child2 %s times", result, result1)
	logger.Info("Parent execution completed", "Result", res)
	return res, nil
}

func (s *ClientDataConverterTestSuite) TestClientDataConverter() {
	s.T().SkipNow() // need to figure out what is going on
	tl := "client-func-data-converter-activity-taskqueue"
	dc := testcore.NewTestDataConverter()
	sdkClient, testWorker := s.startWorkerWithDataConverter(tl, dc)
	defer func() {
		testWorker.Stop()
		sdkClient.Close()
	}()

	id := "client-func-data-converter-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()
	s.Worker().RegisterWorkflow(testDataConverterWorkflow)
	s.Worker().RegisterActivity(testActivity)
	we, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	s.NoError(err)
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("hello_world,hello_world1", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testcore.TestDataConverter) //nolint:revive // unchecked-type-assertion
	s.Equal(1, d.NumOfCallToPayloads)
	s.Equal(1, d.NumOfCallFromPayloads)
}

func (s *ClientDataConverterTestSuite) TestClientDataConverterFailed() {
	s.T().SkipNow()
	tl := "client-func-data-converter-activity-failed-taskqueue"
	sdkClient, newWorker := s.startWorkerWithDataConverter(tl, nil) // mismatch of data converter
	defer func() {
		newWorker.Stop()
		sdkClient.Close()
	}()

	id := "client-func-data-converter-failed-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()

	s.Worker().RegisterWorkflow(testDataConverterWorkflow)
	s.Worker().RegisterActivity(testActivity)
	we, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	s.NoError(err)
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.Error(err)

	// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
	iter := s.SdkClient().GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
	completedAct := 0
	failedAct := 0
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			completedAct++
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
			failedAct++
			s.NotNil(event.GetActivityTaskFailedEventAttributes().GetFailure().GetApplicationFailureInfo())
			s.True(strings.HasPrefix(event.GetActivityTaskFailedEventAttributes().GetFailure().GetMessage(), "unable to decode the activity function input payload with error"))
		}
	}
	s.Equal(1, completedAct)
	s.Equal(1, failedAct)
}

func (s *ClientDataConverterTestSuite) TestClientDataConverterWithChild() {
	s.T().SkipNow()
	dc := testcore.NewTestDataConverter()
	sdkClient, testWorker := s.startWorkerWithDataConverter(childTaskQueue, dc)
	defer func() {
		testWorker.Stop()
		sdkClient.Close()
	}()

	id := "client-func-data-converter-with-child-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()
	s.Worker().RegisterWorkflow(testParentWorkflow)
	s.Worker().RegisterWorkflow(testChildWorkflow)

	we, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
	s.NoError(err)
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("Complete child1 3 times, complete child2 2 times", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testcore.TestDataConverter) //nolint:revive // unchecked-type-assertion
	s.Equal(2, d.NumOfCallToPayloads)
	s.Equal(2, d.NumOfCallFromPayloads)
}

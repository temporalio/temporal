package tests

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/tests/testcore"
)

var (
	ErrInvalidRunCount = errors.New("invalid run count")
)

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

func TestClientDataConverter(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		t.SkipNow() // need to figure out what is going on
		s := testcore.NewEnv(t)

		// Set up SDK client and worker for the main task queue
		mainSdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer mainSdkClient.Close()

		mainTaskQueue := s.Tv().TaskQueue().Name
		mainWorker := worker.New(mainSdkClient, mainTaskQueue, worker.Options{})
		mainWorker.RegisterWorkflow(testDataConverterWorkflow)
		mainWorker.RegisterActivity(testActivity)
		require.NoError(t, mainWorker.Start())
		defer mainWorker.Stop()

		// Set up SDK client and worker for the activity task queue with custom data converter
		tl := "client-func-data-converter-activity-taskqueue"
		dc := testcore.NewTestDataConverter()

		activitySdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:      s.FrontendGRPCAddress(),
			Namespace:     s.Namespace().String(),
			DataConverter: dc,
			Logger:        log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer activitySdkClient.Close()

		activityWorker := worker.New(activitySdkClient, tl, worker.Options{})
		activityWorker.RegisterActivity(testActivity)
		activityWorker.RegisterWorkflow(testChildWorkflow)
		require.NoError(t, activityWorker.Start())
		defer activityWorker.Stop()

		id := "client-func-data-converter-workflow"
		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:                 id,
			TaskQueue:          mainTaskQueue,
			WorkflowRunTimeout: time.Minute,
		}
		ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
		defer cancel()

		we, err := mainSdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
		require.NoError(t, err)
		require.NotNil(t, we)
		require.True(t, we.GetRunID() != "")

		var res string
		err = we.Get(ctx, &res)
		require.NoError(t, err)
		require.Equal(t, "hello_world,hello_world1", res)

		// to ensure custom data converter is used, this number might be different if client changed.
		d := dc.(*testcore.TestDataConverter) //nolint:revive // unchecked-type-assertion
		require.Equal(t, 1, d.NumOfCallToPayloads)
		require.Equal(t, 1, d.NumOfCallFromPayloads)
	})

	t.Run("Failed", func(t *testing.T) {
		t.SkipNow()
		s := testcore.NewEnv(t)

		// Set up SDK client and worker for the main task queue
		mainSdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer mainSdkClient.Close()

		mainTaskQueue := s.Tv().TaskQueue().Name
		mainWorker := worker.New(mainSdkClient, mainTaskQueue, worker.Options{})
		mainWorker.RegisterWorkflow(testDataConverterWorkflow)
		mainWorker.RegisterActivity(testActivity)
		require.NoError(t, mainWorker.Start())
		defer mainWorker.Stop()

		// Set up SDK client and worker for the activity task queue with NO data converter (mismatch)
		tl := "client-func-data-converter-activity-failed-taskqueue"

		activitySdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer activitySdkClient.Close()

		activityWorker := worker.New(activitySdkClient, tl, worker.Options{})
		activityWorker.RegisterActivity(testActivity)
		activityWorker.RegisterWorkflow(testChildWorkflow)
		require.NoError(t, activityWorker.Start())
		defer activityWorker.Stop()

		id := "client-func-data-converter-failed-workflow"
		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:                 id,
			TaskQueue:          mainTaskQueue,
			WorkflowRunTimeout: time.Minute,
		}
		ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
		defer cancel()

		we, err := mainSdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
		require.NoError(t, err)
		require.NotNil(t, we)
		require.True(t, we.GetRunID() != "")

		var res string
		err = we.Get(ctx, &res)
		require.Error(t, err)

		// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
		iter := mainSdkClient.GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
		completedAct := 0
		failedAct := 0
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)
			if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
				completedAct++
			}
			if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
				failedAct++
				require.NotNil(t, event.GetActivityTaskFailedEventAttributes().GetFailure().GetApplicationFailureInfo())
				require.True(t, strings.HasPrefix(event.GetActivityTaskFailedEventAttributes().GetFailure().GetMessage(), "unable to decode the activity function input payload with error"))
			}
		}
		require.Equal(t, 1, completedAct)
		require.Equal(t, 1, failedAct)
	})

	t.Run("WithChild", func(t *testing.T) {
		t.SkipNow()
		s := testcore.NewEnv(t)

		// Set up SDK client and worker for the main task queue
		mainSdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer mainSdkClient.Close()

		mainTaskQueue := s.Tv().TaskQueue().Name
		mainWorker := worker.New(mainSdkClient, mainTaskQueue, worker.Options{})
		mainWorker.RegisterWorkflow(testParentWorkflow)
		mainWorker.RegisterWorkflow(testChildWorkflow)
		require.NoError(t, mainWorker.Start())
		defer mainWorker.Stop()

		// Set up SDK client and worker for the child task queue with custom data converter
		dc := testcore.NewTestDataConverter()

		childSdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:      s.FrontendGRPCAddress(),
			Namespace:     s.Namespace().String(),
			DataConverter: dc,
			Logger:        log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer childSdkClient.Close()

		childWorker := worker.New(childSdkClient, childTaskQueue, worker.Options{})
		childWorker.RegisterActivity(testActivity)
		childWorker.RegisterWorkflow(testChildWorkflow)
		require.NoError(t, childWorker.Start())
		defer childWorker.Stop()

		id := "client-func-data-converter-with-child-workflow"
		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:                 id,
			TaskQueue:          mainTaskQueue,
			WorkflowRunTimeout: time.Minute,
		}
		ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
		defer cancel()

		we, err := mainSdkClient.ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
		require.NoError(t, err)
		require.NotNil(t, we)
		require.True(t, we.GetRunID() != "")

		var res string
		err = we.Get(ctx, &res)
		require.NoError(t, err)
		require.Equal(t, "Complete child1 3 times, complete child2 2 times", res)

		// to ensure custom data converter is used, this number might be different if client changed.
		d := dc.(*testcore.TestDataConverter) //nolint:revive // unchecked-type-assertion
		require.Equal(t, 2, d.NumOfCallToPayloads)
		require.Equal(t, 2, d.NumOfCallFromPayloads)
	})
}

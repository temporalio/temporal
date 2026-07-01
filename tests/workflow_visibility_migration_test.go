package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowVisibilityMigrationSuite struct {
	parallelsuite.Suite[*WorkflowVisibilityMigrationSuite]
}

func TestWorkflowVisibilityMigrationSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowVisibilityMigrationSuite{})
}

func (s *WorkflowVisibilityMigrationSuite) newTestEnv() *testcore.TestEnv {
	return testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityAllowList, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMVisibilityRatio, 0.0),
	)
}

// TODO (rodrigozhou): This test can be removed once CHASM Visibility are the default
func (s *WorkflowVisibilityMigrationSuite) TestWorkflowVisibility_CHASM_Enabled_Mid_WF() {
	// This test verifies that when CHASM Visibility is enabled mid-workflow,
	// visibility still work correctly.
	// 1. Start a workflow with CHASM Visibility disabled
	// 2. Workflow blocks waiting for a signal
	// 3. Enable CHASM Visibility dynamically
	// 4. Send signal to unblock workflow, upsert custom search attribute, and let it complete
	// 5. Verify custom search attributes is correct

	env := s.newTestEnv()

	ctx := s.Context()
	sdkClient := env.SdkClient()

	customKeywordField := temporal.NewSearchAttributeKeyKeyword("CustomKeywordField")

	workflowType := "blockingWorkflow"
	workflowID := env.Tv().WorkflowID()

	// Register workflow that blocks until it receives a signal
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		if err := workflow.UpsertTypedSearchAttributes(
			ctx,
			customKeywordField.ValueSet("bar"),
		); err != nil {
			return 0, err
		}
		return 1, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(30 * time.Second),
		Identity:           s.T().Name(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": sadefs.MustEncodeValue("foo", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			},
		},
	}

	response, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      response.RunId,
	}

	// Wait for workflow to start and reach the blocking point
	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: workflowExecution,
		},
	)
	s.NoError(err)
	csa := descResp.GetWorkflowExecutionInfo().GetSearchAttributes()
	s.Contains(csa.IndexedFields, "CustomKeywordField")
	val, err := sadefs.DecodeValue(
		csa.IndexedFields["CustomKeywordField"],
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		false,
	)
	s.NoError(err)
	s.EqualValues("foo", val)

	// Enable CHASM Visibility mid-workflow
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMVisibilityRatio, 0.9999)

	// Unblock the workflow by sending the continue signal
	_, err = env.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "continue",
		},
	)
	s.NoError(err)

	// Wait for workflow to complete
	run := sdkClient.GetWorkflow(ctx, workflowID, "")
	var result int
	s.NoError(run.Get(ctx, &result))
	s.Equal(1, result)

	descResp, err = env.FrontendClient().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: workflowExecution,
		},
	)
	s.NoError(err)
	csa = descResp.GetWorkflowExecutionInfo().GetSearchAttributes()
	s.Contains(csa.IndexedFields, "CustomKeywordField")
	val, err = sadefs.DecodeValue(
		csa.IndexedFields["CustomKeywordField"],
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		false,
	)
	s.NoError(err)
	s.EqualValues("bar", val)

}

// TODO (rodrigozhou): This test can be removed once CHASM Visibility are the default
func (s *WorkflowVisibilityMigrationSuite) TestWorkflowVisibility_CHASM_Disabled_Mid_WF() {
	// This test verifies that when CHASM Visibility is disabled mid-workflow
	// visibility still work correctly.
	// 1. Start a workflow with CHASM Visibility enabled
	// 2. Workflow blocks waiting for a signal
	// 3. Disable CHASM Visibility dynamically
	// 4. Send signal to unblock workflow, upsert custom search attribute, and let it complete
	// 5. Verify custom search attributes is correct

	env := s.newTestEnv()

	// Enable CHASM for this test (set value just below 1 to ensure Visibility
	// data is still store in mutable state execution info). It's been manually
	// verified that this test starts a workflow with CHASM Visibility enabled.
	// If the test name is modified, a new verification might be needed.
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMVisibilityRatio, 0.9999)

	ctx := s.Context()
	sdkClient := env.SdkClient()

	customKeywordField := temporal.NewSearchAttributeKeyKeyword("CustomKeywordField")

	workflowType := "blockingWorkflow"
	workflowID := env.Tv().WorkflowID()

	// Register workflow that blocks until it receives a signal
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		if err := workflow.UpsertTypedSearchAttributes(
			ctx,
			customKeywordField.ValueSet("bar"),
		); err != nil {
			return 0, err
		}
		return 1, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(30 * time.Second),
		Identity:           s.T().Name(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": sadefs.MustEncodeValue("foo", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			},
		},
	}

	response, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      response.RunId,
	}

	// Wait for workflow to start and reach the blocking point
	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: workflowExecution,
		},
	)
	s.NoError(err)
	csa := descResp.GetWorkflowExecutionInfo().GetSearchAttributes()
	s.Contains(csa.IndexedFields, "CustomKeywordField")
	val, err := sadefs.DecodeValue(
		csa.IndexedFields["CustomKeywordField"],
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		false,
	)
	s.NoError(err)
	s.EqualValues("foo", val)

	// Disable CHASM Visibility mid-workflow
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMVisibilityRatio, 0.0)

	// Unblock the workflow by sending the continue signal
	_, err = env.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "continue",
		},
	)
	s.NoError(err)

	// Wait for workflow to complete
	run := sdkClient.GetWorkflow(ctx, workflowID, "")
	var result int
	s.NoError(run.Get(ctx, &result))
	s.Equal(1, result)

	descResp, err = env.FrontendClient().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: workflowExecution,
		},
	)
	s.NoError(err)
	csa = descResp.GetWorkflowExecutionInfo().GetSearchAttributes()
	s.Contains(csa.IndexedFields, "CustomKeywordField")
	val, err = sadefs.DecodeValue(
		csa.IndexedFields["CustomKeywordField"],
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		false,
	)
	s.NoError(err)
	s.EqualValues("bar", val)

}

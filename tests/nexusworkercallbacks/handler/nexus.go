package handler

import (
	"context"
	"fmt"
	"math"

	"github.com/nexus-rpc/sdk-go/nexus"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
)

const NexusServiceName = "addition-as-a-service"

const AddOperationName = "add"

type AddInput struct {
	A int8
	B int8
}
type AddOutput struct {
	Sum      int8
	Overflow bool
}

func addOperationWorkflow(ctx workflow.Context, input AddInput) (AddOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info(fmt.Sprintf("AddOperationWorkflow(%d, %d)", input.A, input.B))

	sum := int16(input.A) + int16(input.B)
	overflow := sum > math.MaxInt8 || sum < math.MinInt8

	result := AddOutput{
		Sum:      int8(sum),
		Overflow: overflow,
	}
	return result, nil
}

var addOperation = temporalnexus.NewWorkflowRunOperation(
	AddOperationName,
	addOperationWorkflow,
	func(ctx context.Context, input AddInput, options nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		wfStartOpts := client.StartWorkflowOptions{
			ID:        fmt.Sprintf("add-operation_%d_%d", input.A, input.B),
			TaskQueue: HandlerTaskQueue,

			// If two calls come in with the same WorkflowID, have both Nexus operations resolve to the same workflow.
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			// IDEA: It would be neat if we could intercept the duplicate ID condition, and instead
			// lookup the previous operation's result and return that instead of starting a new invocation.
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		}
		return wfStartOpts, nil
	})

package adminbatcher

import (
	"errors"

	"go.temporal.io/sdk/workflow"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/batcher"
)

const (
	WorkflowTypeName = "temporal-sys-admin-batch-workflow"
)

func Workflow(ctx workflow.Context, batchParams *batchspb.BatchOperationInput) (batcher.HeartBeatDetails, error) {
	if batchParams.GetAdminRequest() == nil {
		return batcher.HeartBeatDetails{}, errors.New("admin batch workflow requires an admin request")
	}
	return batcher.RunBatchWorkflow(ctx, batchParams, primitives.AdminBatchActivityTQ)
}

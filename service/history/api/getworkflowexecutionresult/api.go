package getworkflowexecutionresult

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionResultRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	return nil, nil
}

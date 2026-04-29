package deleteexecution

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
)

func Invoke(
	ctx context.Context,
	chasmEngine chasm.Engine,
	request *historyservice.DeleteExecutionRequest,
) (*historyservice.DeleteExecutionResponse, error) {
	key := chasm.ExecutionKey{
		NamespaceID: request.GetNamespaceId(),
		BusinessID:  request.GetExecution().GetWorkflowId(),
		RunID:       request.GetExecution().GetRunId(),
	}
	ref := chasm.NewComponentRefByArchetypeID(key, request.GetArchetypeId())
	if err := chasmEngine.DeleteExecution(ctx, ref, chasm.DeleteExecutionRequest{
		TerminateComponentRequest: chasm.TerminateComponentRequest{
			Reason: request.GetReason(),
		},
	}); err != nil {
		return nil, err
	}
	return &historyservice.DeleteExecutionResponse{}, nil
}

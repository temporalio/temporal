package history

import (
	"context"

	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func (e *outboundQueueStandbyTaskExecutor) verifyStateMachineOutboundTask(
	ctx context.Context,
	task *tasks.StateMachineOutboundTask,
	mutableState historyi.MutableState,
) (any, error) {
	ref, _, err := StateMachineTask(e.shardContext.StateMachineRegistry(), task)
	if err != nil {
		return nil, err
	}
	accessType := hsm.AccessRead
	if task.GetTaskID() != 0 {
		accessType = hsm.AccessWrite
	}
	if err := e.validateNotZombieWorkflow(mutableState, accessType); err != nil {
		return nil, err
	}
	if err := e.validateStateMachineRefState(mutableState, ref, true); err != nil {
		return nil, err
	}
	return &struct{}{}, nil
}

func (e *outboundQueueStandbyTaskExecutor) verifyChasmSideEffectTask(
	ctx context.Context,
	task *tasks.ChasmTask,
	mutableState historyi.MutableState,
) (any, error) {
	inTree, valid, err := validateChasmSideEffectTask(ctx, mutableState, task)
	if err != nil {
		return nil, err
	}
	if !inTree || !valid {
		return nil, nil
	}
	return &struct{}{}, nil
}

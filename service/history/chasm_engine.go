package history

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow/cache"
)

type ChasmEngine struct {
	entityCache     cache.Cache
	shardController shard.Controller
	registry        *chasm.Registry
	config          *configs.Config
}

func NewChasmEngine(
	entityCache cache.Cache,
	shardController shard.Controller,
	registry *chasm.Registry,
	config *configs.Config,
) *ChasmEngine {
	return &ChasmEngine{
		entityCache:     entityCache,
		shardController: shardController,
		registry:        registry,
		config:          config,
	}
}

func (e *ChasmEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (updatedRef chasm.ComponentRef, retError error) {

	shardContext, executionLease, err := e.getExecutionLease(ctx, ref)
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	defer func() {
		executionLease.GetReleaseFn()(retError)
	}()

	mutableState := executionLease.GetMutableState()
	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return chasm.ComponentRef{}, serviceerror.NewInternal(
			fmt.Sprintf(
				"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
				mutableState.ChasmTree(),
				&chasm.Node{},
			),
		)
	}

	mutableContext := chasm.NewMutableContext(ctx, chasmTree)
	component, err := chasmTree.Component(mutableContext, ref)
	if err != nil {
		return chasm.ComponentRef{}, err
	}

	if err := updateFn(mutableContext, component); err != nil {
		return chasm.ComponentRef{}, err
	}

	// TODO: Support WithSpeculative() TransitionOption.

	if err := executionLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return chasm.ComponentRef{}, err
	}

	newRef, ok := mutableContext.RefC(component)
	if !ok {
		return chasm.ComponentRef{}, serviceerror.NewInternal(
			fmt.Sprintf("component not found in the new component tree after mutation, componentRef: %+v", ref),
		)
	}

	return newRef, nil
}

func (e *ChasmEngine) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (retError error) {
	_, executionLease, err := e.getExecutionLease(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		// Always release the lease with nil error since this is a read only operation
		// So even if it fails, we don't need to clear and reload mutable state.
		executionLease.GetReleaseFn()(nil)
	}()

	chasmTree, ok := executionLease.GetMutableState().ChasmTree().(*chasm.Node)
	if !ok {
		return serviceerror.NewInternal(
			fmt.Sprintf(
				"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
				executionLease.GetMutableState().ChasmTree(),
				&chasm.Node{},
			),
		)
	}

	chasmContext := chasm.NewContext(ctx, chasmTree)
	component, err := chasmTree.Component(chasmContext, ref)
	if err != nil {
		return err
	}

	return readFn(chasmContext, component)
}

func (e *ChasmEngine) getExecutionLease(
	ctx context.Context,
	ref chasm.ComponentRef,
) (historyi.ShardContext, api.WorkflowLease, error) {
	shardID, err := ref.ShardID(e.registry, e.config.NumberOfShards)
	if err != nil {
		return nil, nil, err
	}

	shardContext, err := e.shardController.GetShardByID(shardID)
	if err != nil {
		return nil, nil, err
	}

	consistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		e.entityCache,
	)

	lockPriority := locks.PriorityHigh
	callerType := headers.GetCallerInfo(ctx).CallerType
	if callerType == headers.CallerTypeBackground || callerType == headers.CallerTypePreemptable {
		lockPriority = locks.PriorityLow
	}

	var staleReferenceErr error
	entityLease, err := consistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState historyi.MutableState) bool {
			err := mutableState.ChasmTree().IsStale(ref)
			if errors.Is(err, consts.ErrStaleState) {
				return false
			}

			// Reference itself might be stale.
			// No need to reload mutable state in this case, but request should be failed.
			staleReferenceErr = err
			return true
		},
		definition.NewWorkflowKey(
			ref.EntityKey.NamespaceID,
			ref.EntityKey.BusinessID,
			ref.EntityKey.EntityID,
		),
		lockPriority,
	)
	if err == nil && staleReferenceErr != nil {
		entityLease.GetReleaseFn()(nil)
		err = staleReferenceErr
	}

	return shardContext, entityLease, err
}

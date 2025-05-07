package history

import (
	"context"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

type (
	ChasmEngine struct {
		entityCache     cache.Cache
		shardController shard.Controller
		registry        *chasm.Registry
		config          *configs.Config
	}

	newEntityParams struct {
		entityRef     chasm.ComponentRef
		entityContext historyi.WorkflowContext
		mutableState  historyi.MutableState
		snapshot      *persistence.WorkflowSnapshot
		events        []*persistence.WorkflowEvents
	}

	currentRunInfo struct {
		createRequestID string
		*persistence.CurrentWorkflowConditionFailedError
	}
)

var defaultTransitionOptions = chasm.TransitionOptions{
	ReusePolicy:    chasm.BusinessIDReusePolicyAllowDuplicate,
	ConflictPolicy: chasm.BusinessIDConflictPolicyFail,
	RequestID:      "",
	Speculative:    false,
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

func (e *ChasmEngine) NewEntity(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	opts ...chasm.TransitionOption,
) (newEntityRef chasm.ComponentRef, retErr error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(entityRef)
	if err != nil {
		return chasm.ComponentRef{}, err
	}

	currentEntityReleaseFn, err := e.lockCurrentEntity(
		ctx,
		shardContext,
		namespace.ID(entityRef.NamespaceID),
		entityRef.BusinessID,
	)
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	defer func() {
		currentEntityReleaseFn(retErr)
	}()

	newEntityParams, err := e.createNewEntity(
		ctx,
		shardContext,
		entityRef,
		newFn,
		options,
	)
	if err != nil {
		return chasm.ComponentRef{}, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newEntityParams,
	)
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	if !hasCurrentRun {
		return newEntityParams.entityRef, nil
	}

	return e.handleEntityConflict(
		ctx,
		shardContext,
		newEntityParams,
		currentRunInfo,
		options,
	)
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
		return chasm.ComponentRef{}, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
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

	newRef, err := mutableContext.Ref(component)
	if err != nil {
		return chasm.ComponentRef{}, serviceerror.NewInternalf("componentRef: %+v: %s", ref, err)
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
		return serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			executionLease.GetMutableState().ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewContext(ctx, chasmTree)
	component, err := chasmTree.Component(chasmContext, ref)
	if err != nil {
		return err
	}

	return readFn(chasmContext, component)
}

func (e *ChasmEngine) constructTransitionOptions(
	opts ...chasm.TransitionOption,
) chasm.TransitionOptions {
	options := defaultTransitionOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.RequestID == "" {
		options.RequestID = primitives.NewUUID().String()
	}
	return options
}

func (e *ChasmEngine) lockCurrentEntity(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	businessID string,
) (historyi.ReleaseWorkflowContextFunc, error) {
	currentEntityReleaseFn, err := e.entityCache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		shardContext,
		namespaceID,
		businessID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}

	return currentEntityReleaseFn, nil
}

func (e *ChasmEngine) createNewEntity(
	ctx context.Context,
	shardContext historyi.ShardContext,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	options chasm.TransitionOptions,
) (newEntityParams, error) {
	entityRef.EntityID = primitives.NewUUID().String()

	entityKey := entityRef.EntityKey
	nsRegistry := shardContext.GetNamespaceRegistry()
	nsEntry, err := nsRegistry.GetNamespaceByID(namespace.ID(entityKey.NamespaceID))
	if err != nil {
		return newEntityParams{}, err
	}

	mutableState := workflow.NewMutableState(
		shardContext,
		shardContext.GetEventsCache(),
		shardContext.GetLogger(),
		nsEntry,
		entityKey.BusinessID,
		entityKey.EntityID,
		shardContext.GetTimeSource().Now(),
	)
	mutableState.AttachRequestID(options.RequestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, 0)

	chasmTree := mutableState.ChasmTree().(*chasm.Node)
	chasmContext := chasm.NewMutableContext(ctx, chasmTree)
	rootComponent, err := newFn(chasmContext)
	if err != nil {
		return newEntityParams{}, err
	}
	chasmTree.SetRootComponent(rootComponent)

	snapshot, events, err := mutableState.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
	if err != nil {
		return newEntityParams{}, err
	}
	if len(events) != 0 {
		return newEntityParams{}, serviceerror.NewInternal(
			fmt.Sprintf("CHASM framework does not support events yet, found events for new run: %v", events),
		)
	}

	return newEntityParams{
		entityRef: entityRef,
		entityContext: workflow.NewContext(
			e.config,
			definition.NewWorkflowKey(
				entityKey.NamespaceID,
				entityKey.BusinessID,
				entityKey.EntityID,
			),
			shardContext.GetLogger(),
			shardContext.GetThrottledLogger(),
			shardContext.GetMetricsHandler(),
		),
		mutableState: mutableState,
		snapshot:     snapshot,
		events:       events,
	}, nil
}

func (e *ChasmEngine) persistAsBrandNew(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
) (currentRunInfo, bool, error) {
	err := newEntityParams.entityContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeBrandNew,
		"", // previousRunID
		0,  // prevlastWriteVersion
		newEntityParams.mutableState,
		newEntityParams.snapshot,
		newEntityParams.events,
	)
	if err == nil {
		return currentRunInfo{}, false, nil
	}

	var currentRunConditionFailedError *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &currentRunConditionFailedError) ||
		len(currentRunConditionFailedError.RunID) == 0 {
		return currentRunInfo{}, false, err
	}

	createRequestID := ""
	for requestID, info := range currentRunConditionFailedError.RequestIDs {
		if info.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			createRequestID = requestID
		}
	}
	return currentRunInfo{
		createRequestID:                     createRequestID,
		CurrentWorkflowConditionFailedError: currentRunConditionFailedError,
	}, true, nil
}

func (e *ChasmEngine) handleEntityConflict(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	options chasm.TransitionOptions,
) (chasm.ComponentRef, error) {
	// Check if this a retired request using requestID.
	if _, ok := currentRunInfo.RequestIDs[options.RequestID]; ok {
		newEntityParams.entityRef.EntityID = currentRunInfo.RunID
		return newEntityParams.entityRef, nil
	}

	// Verify failover version and make sure it won't go backwards even if the case of split brain.
	mutableState := newEntityParams.mutableState
	nsEntry := mutableState.GetNamespaceEntry()
	if mutableState.GetCurrentVersion() < currentRunInfo.LastWriteVersion {
		clusterMetadata := shardContext.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(
			nsEntry.IsGlobalNamespace(),
			currentRunInfo.LastWriteVersion,
		)
		return chasm.ComponentRef{}, serviceerror.NewNamespaceNotActive(
			nsEntry.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}

	switch currentRunInfo.State {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return e.handleConflictPolicy(ctx, shardContext, newEntityParams, currentRunInfo, options.ConflictPolicy)
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return e.handleReusePolicy(ctx, shardContext, newEntityParams, currentRunInfo, options.ReusePolicy)
	default:
		return chasm.ComponentRef{}, serviceerror.NewInternal(
			fmt.Sprintf("unexpected current run state when creating new entity: %v", currentRunInfo.State),
		)
	}
}

func (e *ChasmEngine) handleConflictPolicy(
	_ context.Context,
	_ historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	conflictPolicy chasm.BusinessIDConflictPolicy,
) (chasm.ComponentRef, error) {
	switch conflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.ComponentRef{}, serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf(
				"CHASM execution still running. BusinessID: %s, RunID: %s, ID Conflict Policy: %v",
				newEntityParams.entityRef.EntityKey.BusinessID,
				currentRunInfo.RunID,
				conflictPolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	case chasm.BusinessIDConflictPolicyTermiateExisting:
		// TODO: handle BusinessIDConflictPolicyTermiateExisting
		return chasm.ComponentRef{}, serviceerror.NewUnimplemented("ID Conflict Policy Terminate Existing is not yet supported")
	// case chasm.BusinessIDConflictPolicyUseExisting:
	// 	return chasm.ComponentRef{}, serviceerror.NewUnimplemented("ID Conflict Policy Use Existing is not yet supported")
	default:
		return chasm.ComponentRef{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID conflict policy for newEntity: %v", conflictPolicy),
		)
	}
}

func (e *ChasmEngine) handleReusePolicy(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	reusePolicy chasm.BusinessIDReusePolicy,
) (chasm.ComponentRef, error) {
	switch reusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
		// No more check needed.
		// Fallthrough to persist the new entity as current run.
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if _, ok := consts.FailedWorkflowStatuses[currentRunInfo.Status]; !ok {
			return chasm.ComponentRef{}, serviceerror.NewWorkflowExecutionAlreadyStarted(
				fmt.Sprintf(
					"CHASM execution already completed successfully. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
					newEntityParams.entityRef.EntityKey.BusinessID,
					currentRunInfo.RunID,
					reusePolicy,
				),
				currentRunInfo.createRequestID,
				currentRunInfo.RunID,
			)
		}
		// Fallthrough to persist the new entity as current run.
	case chasm.BusinessIDReusePolicyRejectDuplicate:
		return chasm.ComponentRef{}, serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf(
				"CHASM execution already finished. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
				newEntityParams.entityRef.EntityKey.BusinessID,
				currentRunInfo.RunID,
				reusePolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	default:
		return chasm.ComponentRef{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID reuse policy for newEntity: %v", reusePolicy),
		)
	}

	err := newEntityParams.entityContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunInfo.RunID,
		currentRunInfo.LastWriteVersion,
		newEntityParams.mutableState,
		newEntityParams.snapshot,
		newEntityParams.events,
	)
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	return newEntityParams.entityRef, nil
}

func (e *ChasmEngine) getShardContext(
	ref chasm.ComponentRef,
) (historyi.ShardContext, error) {
	shardID, err := ref.ShardID(e.registry, e.config.NumberOfShards)
	if err != nil {
		return nil, err
	}

	return e.shardController.GetShardByID(shardID)
}

func (e *ChasmEngine) getExecutionLease(
	ctx context.Context,
	ref chasm.ComponentRef,
) (historyi.ShardContext, api.WorkflowLease, error) {
	shardContext, err := e.getShardContext(ref)
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

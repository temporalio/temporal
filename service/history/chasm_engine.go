package history

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	ChasmEngine struct {
		executionCache  cache.Cache
		shardController shard.Controller
		registry        *chasm.Registry
		config          *configs.Config
		visibilityMgr   manager.VisibilityManager
	}

	newExecutionParams struct {
		executionRef     chasm.ComponentRef
		executionContext historyi.WorkflowContext
		mutableState     historyi.MutableState
		snapshot         *persistence.WorkflowSnapshot
		events           []*persistence.WorkflowEvents
	}

	currentExecutionInfo struct {
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

var ChasmEngineModule = fx.Options(
	fx.Provide(newChasmEngine),
	fx.Provide(func(impl *ChasmEngine) chasm.Engine { return impl }),
	fx.Invoke(func(impl *ChasmEngine, shardController shard.Controller) {
		impl.SetShardController(shardController)
	}),
)

func newChasmEngine(
	executionCache cache.Cache,
	registry *chasm.Registry,
	config *configs.Config,
	visibilityMgr manager.VisibilityManager,
) *ChasmEngine {
	return &ChasmEngine{
		executionCache: executionCache,
		registry:       registry,
		config:         config,
		visibilityMgr:  visibilityMgr,
	}
}

// This is for breaking fx cycle dependency.
// ChasmEngine -> ShardController -> ShardContextFactory -> HistoryEngineFactory -> QueueFactory -> ChasmEngine
func (e *ChasmEngine) SetShardController(
	shardController shard.Controller,
) {
	e.shardController = shardController
}

func (e *ChasmEngine) NewExecution(
	ctx context.Context,
	executionRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	opts ...chasm.TransitionOption,
) (executionKey chasm.ExecutionKey, newExecutionRef []byte, retErr error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(executionRef)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}

	archetypeID, err := executionRef.ArchetypeID(e.registry)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}

	currentExecutionReleaseFn, err := e.lockCurrentExecution(
		ctx,
		shardContext,
		namespace.ID(executionRef.NamespaceID),
		executionRef.BusinessID,
		archetypeID,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}
	defer func() {
		currentExecutionReleaseFn(retErr)
	}()

	newExecutionParams, err := e.createNewExecution(
		ctx,
		shardContext,
		executionRef,
		archetypeID,
		newFn,
		options,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newExecutionParams,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}
	if !hasCurrentRun {
		serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
		if err != nil {
			return chasm.ExecutionKey{}, nil, err
		}
		return newExecutionParams.executionRef.ExecutionKey, serializedRef, nil
	}

	return e.handleExecutionConflict(
		ctx,
		shardContext,
		newExecutionParams,
		currentRunInfo,
		options,
	)
}

func (e *ChasmEngine) UpdateWithNewExecution(
	ctx context.Context,
	executionRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (newExecutionKey chasm.ExecutionKey, newExecutionRef []byte, retError error) {
	return chasm.ExecutionKey{}, nil, serviceerror.NewUnimplemented("UpdateWithNewExecution is not yet supported")
}

func (e *ChasmEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (updatedRef []byte, retError error) {

	shardContext, executionLease, err := e.getExecutionLease(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer func() {
		executionLease.GetReleaseFn()(retError)
	}()

	mutableState := executionLease.GetMutableState()
	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return nil, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
		)
	}

	mutableContext := chasm.NewMutableContext(ctx, chasmTree)
	component, err := chasmTree.Component(mutableContext, ref)
	if err != nil {
		return nil, err
	}

	if err := updateFn(mutableContext, component); err != nil {
		return nil, err
	}

	// TODO: Support WithSpeculative() TransitionOption.

	if err := executionLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return nil, err
	}

	newSerializedRef, err := mutableContext.Ref(component)
	if err != nil {
		return nil, serviceerror.NewInternalf("componentRef: %+v: %s", ref, err)
	}

	return newSerializedRef, nil
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

func (e *ChasmEngine) PollComponent(
	ctx context.Context,
	executionRef chasm.ComponentRef,
	predicateFn func(chasm.Context, chasm.Component) (any, bool, error),
	operationFn func(chasm.MutableContext, chasm.Component, any) error,
	opts ...chasm.TransitionOption,
) (newExecutionRef []byte, retError error) {
	return nil, serviceerror.NewUnimplemented("PollComponent is not yet supported")
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

func (e *ChasmEngine) lockCurrentExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	businessID string,
	archetypeID chasm.ArchetypeID,
) (historyi.ReleaseWorkflowContextFunc, error) {
	currentExecutionReleaseFn, err := e.executionCache.GetOrCreateCurrentExecution(
		ctx,
		shardContext,
		namespaceID,
		businessID,
		archetypeID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}

	return currentExecutionReleaseFn, nil
}

func (e *ChasmEngine) createNewExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionRef chasm.ComponentRef,
	archetypeID chasm.ArchetypeID,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	options chasm.TransitionOptions,
) (newExecutionParams, error) {
	executionRef.RunID = primitives.NewUUID().String()

	executionKey := executionRef.ExecutionKey
	nsRegistry := shardContext.GetNamespaceRegistry()
	nsEntry, err := nsRegistry.GetNamespaceByID(namespace.ID(executionKey.NamespaceID))
	if err != nil {
		return newExecutionParams{}, err
	}

	mutableState := workflow.NewMutableState(
		shardContext,
		shardContext.GetEventsCache(),
		shardContext.GetLogger(),
		nsEntry,
		executionKey.BusinessID,
		executionKey.RunID,
		shardContext.GetTimeSource().Now(),
	)
	mutableState.AttachRequestID(options.RequestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, 0)

	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return newExecutionParams{}, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewMutableContext(ctx, chasmTree)
	rootComponent, err := newFn(chasmContext)
	if err != nil {
		return newExecutionParams{}, err
	}
	chasmTree.SetRootComponent(rootComponent)

	snapshot, events, err := mutableState.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
	if err != nil {
		return newExecutionParams{}, err
	}
	if len(events) != 0 {
		return newExecutionParams{}, serviceerror.NewInternal(
			fmt.Sprintf("CHASM framework does not support events yet, found events for new run: %v", events),
		)
	}

	return newExecutionParams{
		executionRef: executionRef,
		executionContext: workflow.NewContext(
			e.config,
			definition.NewWorkflowKey(
				executionKey.NamespaceID,
				executionKey.BusinessID,
				executionKey.RunID,
			),
			archetypeID,
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
	newExecutionParams newExecutionParams,
) (currentExecutionInfo, bool, error) {
	err := newExecutionParams.executionContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeBrandNew,
		"", // previousRunID
		0,  // prevlastWriteVersion
		newExecutionParams.mutableState,
		newExecutionParams.snapshot,
		newExecutionParams.events,
	)
	if err == nil {
		return currentExecutionInfo{}, false, nil
	}

	var currentRunConditionFailedError *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &currentRunConditionFailedError) ||
		len(currentRunConditionFailedError.RunID) == 0 {
		return currentExecutionInfo{}, false, err
	}

	createRequestID := ""
	for requestID, info := range currentRunConditionFailedError.RequestIDs {
		if info.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			createRequestID = requestID
		}
	}
	return currentExecutionInfo{
		createRequestID:                     createRequestID,
		CurrentWorkflowConditionFailedError: currentRunConditionFailedError,
	}, true, nil
}

func (e *ChasmEngine) handleExecutionConflict(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newExecutionParams newExecutionParams,
	currentRunInfo currentExecutionInfo,
	options chasm.TransitionOptions,
) (chasm.ExecutionKey, []byte, error) {
	// Check if this a retired request using requestID.
	if _, ok := currentRunInfo.RequestIDs[options.RequestID]; ok {
		newExecutionParams.executionRef.RunID = currentRunInfo.RunID
		serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
		if err != nil {
			return chasm.ExecutionKey{}, nil, err
		}
		return newExecutionParams.executionRef.ExecutionKey, serializedRef, nil
	}

	// Verify failover version and make sure it won't go backwards even if the case of split brain.
	mutableState := newExecutionParams.mutableState
	nsEntry := mutableState.GetNamespaceEntry()
	if mutableState.GetCurrentVersion() < currentRunInfo.LastWriteVersion {
		clusterMetadata := shardContext.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(
			nsEntry.IsGlobalNamespace(),
			currentRunInfo.LastWriteVersion,
		)
		return chasm.ExecutionKey{}, nil, serviceerror.NewNamespaceNotActive(
			nsEntry.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}

	switch currentRunInfo.State {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return e.handleConflictPolicy(ctx, shardContext, newExecutionParams, currentRunInfo, options.ConflictPolicy)
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return e.handleReusePolicy(ctx, shardContext, newExecutionParams, currentRunInfo, options.ReusePolicy)
	default:
		return chasm.ExecutionKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unexpected current run state when creating new execution: %v", currentRunInfo.State),
		)
	}
}

func (e *ChasmEngine) handleConflictPolicy(
	_ context.Context,
	_ historyi.ShardContext,
	newExecutionParams newExecutionParams,
	currentRunInfo currentExecutionInfo,
	conflictPolicy chasm.BusinessIDConflictPolicy,
) (chasm.ExecutionKey, []byte, error) {
	switch conflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.ExecutionKey{}, nil, chasm.NewExecutionAlreadyStartedErr(
			fmt.Sprintf(
				"CHASM execution still running. BusinessID: %s, RunID: %s, ID Conflict Policy: %v",
				newExecutionParams.executionRef.BusinessID,
				currentRunInfo.RunID,
				conflictPolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	case chasm.BusinessIDConflictPolicyTerminateExisting:
		// TODO: handle BusinessIDConflictPolicyTerminateExisting and update TestNewExecution_ConflictPolicy_TerminateExisting.
		//
		// Today's state-based replication logic can not existly handle this policy correctly
		// (or any operation that close and starts a new run in one transaction).
		// The termination and creation of new run can not be replicated transactionally.
		//
		// The main blocker is that state-based replication works on the current state,
		// and we may have a chain of runs all created via TerminateExisting policy, meaning
		// replication has to replicated all of them transactionally.
		// We need a way to break this chain into consistent pieces and replicate them one by one.
		return chasm.ExecutionKey{}, nil, serviceerror.NewUnimplemented("ID Conflict Policy Terminate Existing is not yet supported")
	case chasm.BusinessIDConflictPolicyUseExisting:
		existingExecutionRef := newExecutionParams.executionRef
		existingExecutionRef.RunID = currentRunInfo.RunID
		serializedRef, err := existingExecutionRef.Serialize(e.registry)
		if err != nil {
			return chasm.ExecutionKey{}, nil, err
		}
		return existingExecutionRef.ExecutionKey, serializedRef, nil
	default:
		return chasm.ExecutionKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID conflict policy for NewExecution: %v", conflictPolicy),
		)
	}
}

func (e *ChasmEngine) handleReusePolicy(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newExecutionParams newExecutionParams,
	currentRunInfo currentExecutionInfo,
	reusePolicy chasm.BusinessIDReusePolicy,
) (chasm.ExecutionKey, []byte, error) {
	switch reusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
		// No more check needed.
		// Fallthrough to persist the new execution as current run.
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if _, ok := consts.FailedWorkflowStatuses[currentRunInfo.Status]; !ok {
			return chasm.ExecutionKey{}, nil, chasm.NewExecutionAlreadyStartedErr(
				fmt.Sprintf(
					"CHASM execution already completed successfully. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
					newExecutionParams.executionRef.BusinessID,
					currentRunInfo.RunID,
					reusePolicy,
				),
				currentRunInfo.createRequestID,
				currentRunInfo.RunID,
			)
		}
		// Fallthrough to persist the new execution as current run.
	case chasm.BusinessIDReusePolicyRejectDuplicate:
		return chasm.ExecutionKey{}, nil, chasm.NewExecutionAlreadyStartedErr(
			fmt.Sprintf(
				"CHASM execution already finished. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
				newExecutionParams.executionRef.BusinessID,
				currentRunInfo.RunID,
				reusePolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	default:
		return chasm.ExecutionKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID reuse policy for NewExecution: %v", reusePolicy),
		)
	}

	err := newExecutionParams.executionContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunInfo.RunID,
		currentRunInfo.LastWriteVersion,
		newExecutionParams.mutableState,
		newExecutionParams.snapshot,
		newExecutionParams.events,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}

	serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}
	return newExecutionParams.executionRef.ExecutionKey, serializedRef, nil
}

func (e *ChasmEngine) getShardContext(
	ref chasm.ComponentRef,
) (historyi.ShardContext, error) {
	shardingKey, err := ref.ShardingKey(e.registry)
	if err != nil {
		return nil, err
	}
	shardID := common.ShardingKeyToShard(
		shardingKey,
		e.config.NumberOfShards,
	)

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
		e.executionCache,
	)

	lockPriority := locks.PriorityHigh
	callerType := headers.GetCallerInfo(ctx).CallerType
	if callerType == headers.CallerTypeBackgroundHigh || callerType == headers.CallerTypeBackgroundLow || callerType == headers.CallerTypePreemptable {
		lockPriority = locks.PriorityLow
	}

	archetypeID, err := ref.ArchetypeID(e.registry)
	if err != nil {
		return nil, nil, err
	}

	var staleReferenceErr error
	executionLease, err := consistencyChecker.GetChasmLeaseWithConsistencyCheck(
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
			ref.NamespaceID,
			ref.BusinessID,
			ref.RunID,
		),
		archetypeID,
		lockPriority,
	)
	if err == nil && staleReferenceErr != nil {
		executionLease.GetReleaseFn()(nil)
		err = staleReferenceErr
	}

	return shardContext, executionLease, err
}

// ListExecutions implements the Engine interface for visibility queries.
func (e *ChasmEngine) ListExecutions(
	ctx context.Context,
	archetypeType reflect.Type,
	request *chasm.ListExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	archetypeID, ok := e.registry.ArchetypeIDOf(archetypeType)
	if !ok {
		return nil, serviceerror.NewInternal("unknown chasm component type: " + archetypeType.String())
	}

	visReq := &manager.ListChasmExecutionsRequest{
		ArchetypeID:   archetypeID,
		NamespaceID:   namespace.ID(request.NamespaceID),
		Namespace:     namespace.Name(request.NamespaceName),
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
		Query:         request.Query,
	}

	return e.visibilityMgr.ListChasmExecutions(ctx, visReq)
}

// CountExecutions implements the Engine interface for visibility queries.
func (e *ChasmEngine) CountExecutions(
	ctx context.Context,
	archetypeType reflect.Type,
	request *chasm.CountExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	archetypeID, ok := e.registry.ArchetypeIDOf(archetypeType)
	if !ok {
		return nil, serviceerror.NewInternal("unknown chasm component type: " + archetypeType.String())
	}

	visReq := &manager.CountChasmExecutionsRequest{
		ArchetypeID: archetypeID,
		NamespaceID: namespace.ID(request.NamespaceID),
		Namespace:   namespace.Name(request.NamespaceName),
		Query:       request.Query,
	}

	return e.visibilityMgr.CountChasmExecutions(ctx, visReq)
}

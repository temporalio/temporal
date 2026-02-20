package history

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
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
		notifier        *ChasmNotifier
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
	fx.Provide(NewChasmNotifier),
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
	notifier *ChasmNotifier,
) *ChasmEngine {
	return &ChasmEngine{
		executionCache: executionCache,
		registry:       registry,
		config:         config,
		notifier:       notifier,
	}
}

// This is for breaking fx cycle dependency.
// ChasmEngine -> ShardController -> ShardContextFactory -> HistoryEngineFactory -> QueueFactory -> ChasmEngine
func (e *ChasmEngine) SetShardController(
	shardController shard.Controller,
) {
	e.shardController = shardController
}

func (e *ChasmEngine) NotifyExecution(key chasm.ExecutionKey) {
	e.notifier.Notify(key)
}

func (e *ChasmEngine) StartExecution(
	ctx context.Context,
	executionRef chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.Component, error),
	opts ...chasm.TransitionOption,
) (result chasm.StartExecutionResult, retErr error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(executionRef)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	archetypeID, err := executionRef.ArchetypeID(e.registry)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	if executionRef.RunID != "" {
		return chasm.StartExecutionResult{}, serviceerror.NewUnimplemented("setting runID is not supported for StartExecution")
	}

	currentExecutionReleaseFn, err := e.lockCurrentExecution(
		ctx,
		shardContext,
		namespace.ID(executionRef.NamespaceID),
		executionRef.BusinessID,
		archetypeID,
	)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}
	defer func() {
		currentExecutionReleaseFn(retErr)
	}()

	newExecutionParams, err := e.createNewExecution(
		ctx,
		shardContext,
		executionRef,
		archetypeID,
		startFn,
		options,
	)
	if err != nil {
		return chasm.StartExecutionResult{}, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newExecutionParams,
	)
	if err != nil {
		// Even though Created is false, it's not guaranteed the execution wasn't created.
		// The persistence layer writes history events outside the main transaction, so on errors
		// like network timeouts etc., the operation outcome is ambiguous.
		return chasm.StartExecutionResult{}, err
	}
	if !hasCurrentRun {
		serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
		if err != nil {
			// Created is true here because persistAsBrandNew succeeded, but we failed to serialize the ref.
			return chasm.StartExecutionResult{
				ExecutionKey: newExecutionParams.executionRef.ExecutionKey,
				Created:      true,
			}, err
		}
		return chasm.StartExecutionResult{
			ExecutionKey: newExecutionParams.executionRef.ExecutionKey,
			ExecutionRef: serializedRef,
			Created:      true,
		}, nil
	}

	return e.handleExecutionConflict(
		ctx,
		shardContext,
		newExecutionParams,
		currentRunInfo,
		options,
	)
}

func (e *ChasmEngine) UpdateWithStartExecution(
	ctx context.Context,
	executionRef chasm.ComponentRef,
	startFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (result chasm.EngineUpdateWithStartExecutionResult, retError error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(executionRef)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	archetypeID, err := executionRef.ArchetypeID(e.registry)
	if err != nil {
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}

	if executionRef.RunID != "" {
		return chasm.EngineUpdateWithStartExecutionResult{}, serviceerror.NewUnimplemented("setting runID is not supported for UpdateWithStartExecution")
	}

	_, executionLease, err := e.getExecutionLease(ctx, executionRef)
	switch err.(type) {
	case nil:
		defer func() {
			executionLease.GetReleaseFn()(retError)
		}()

		if executionLease.GetMutableState().IsWorkflowExecutionRunning() {
			executionKey, executionRef, err := e.updateExecution(ctx, shardContext, executionLease, executionRef, updateFn)
			if err != nil {
				return chasm.EngineUpdateWithStartExecutionResult{}, err
			}
			return chasm.EngineUpdateWithStartExecutionResult{
				ExecutionKey: executionKey,
				ExecutionRef: executionRef,
				Created:      false,
			}, nil
		}

		executionKey, executionRef, created, err := e.startNewForClosedExecution(
			ctx,
			shardContext,
			executionLease,
			executionRef,
			archetypeID,
			startFn,
			updateFn,
			options,
		)
		if err != nil {
			return chasm.EngineUpdateWithStartExecutionResult{}, err
		}
		return chasm.EngineUpdateWithStartExecutionResult{
			ExecutionKey: executionKey,
			ExecutionRef: executionRef,
			Created:      created,
		}, nil
	case *serviceerror.NotFound:
		executionKey, executionRef, created, err := e.startAndUpdateExecution(
			ctx,
			shardContext,
			executionRef,
			archetypeID,
			startFn,
			updateFn,
			options,
		)
		if err != nil {
			return chasm.EngineUpdateWithStartExecutionResult{}, err
		}
		return chasm.EngineUpdateWithStartExecutionResult{
			ExecutionKey: executionKey,
			ExecutionRef: executionRef,
			Created:      created,
		}, nil
	default:
		return chasm.EngineUpdateWithStartExecutionResult{}, err
	}
}

func (e *ChasmEngine) updateExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionLease api.WorkflowLease,
	executionRef chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
) (chasm.ExecutionKey, []byte, error) {
	workflowKey := executionLease.GetContext().GetWorkflowKey()
	actualRef := executionRef
	actualRef.RunID = workflowKey.RunID

	serializedRef, err := e.applyUpdateWithLease(ctx, shardContext, executionLease, actualRef, updateFn)
	if err != nil {
		return chasm.ExecutionKey{}, nil, err
	}
	return actualRef.ExecutionKey, serializedRef, nil
}

// startNewForClosedExecution handles starting a new execution when we already hold a lease on a
// closed execution. It creates the new execution first, then delegates to handleExecutionConflict
// for policy checks and persistence.
func (e *ChasmEngine) startNewForClosedExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionLease api.WorkflowLease,
	executionRef chasm.ComponentRef,
	archetypeID chasm.ArchetypeID,
	startFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	options chasm.TransitionOptions,
) (chasm.ExecutionKey, []byte, bool, error) {
	newExecutionParams, err := e.createNewExecutionWithUpdate(
		ctx,
		shardContext,
		executionRef,
		archetypeID,
		startFn,
		updateFn,
		options,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, false, err
	}

	currentRunInfo := currentExecutionInfoFromMutableState(executionLease.GetMutableState())

	result, err := e.handleExecutionConflict(ctx, shardContext, newExecutionParams, currentRunInfo, options)
	return result.ExecutionKey, result.ExecutionRef, result.Created, err
}

// applyUpdateWithLease applies an update function to a component within a held lease,
// persists the changes, and returns the serialized component reference.
func (e *ChasmEngine) applyUpdateWithLease(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionLease api.WorkflowLease,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
) ([]byte, error) {
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

	serializedRef, err := mutableContext.Ref(component)
	if err != nil {
		return nil, serviceerror.NewInternalf("componentRef: %+v: %s", ref, err)
	}

	return serializedRef, nil
}

func (e *ChasmEngine) startAndUpdateExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionRef chasm.ComponentRef,
	archetypeID chasm.ArchetypeID,
	startFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	options chasm.TransitionOptions,
) (retKey chasm.ExecutionKey, retRef []byte, created bool, retErr error) {
	currentExecutionReleaseFn, err := e.lockCurrentExecution(
		ctx,
		shardContext,
		namespace.ID(executionRef.NamespaceID),
		executionRef.BusinessID,
		archetypeID,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, false, err
	}
	defer func() {
		currentExecutionReleaseFn(retErr)
	}()

	newExecutionParams, err := e.createNewExecutionWithUpdate(
		ctx,
		shardContext,
		executionRef,
		archetypeID,
		startFn,
		updateFn,
		options,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, false, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newExecutionParams,
	)
	if err != nil {
		return chasm.ExecutionKey{}, nil, false, err
	}
	if hasCurrentRun {
		return chasm.ExecutionKey{}, nil, false, currentRunInfo.CurrentWorkflowConditionFailedError
	}

	serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)

	return newExecutionParams.executionRef.ExecutionKey, serializedRef, true, err
}

// UpdateComponent applies updateFn to the component identified by the supplied component reference,
// returning the new component reference corresponding to the transition. An error is returned if
// the state transition specified by the supplied component reference is inconsistent with execution
// transition history. opts are currently ignored.
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

	return e.applyUpdateWithLease(ctx, shardContext, executionLease, ref, updateFn)
}

// ReadComponent evaluates readFn against the current state of the component identified by the
// supplied component reference. An error is returned if the state transition specified by the
// component reference is inconsistent with execution transition history. opts are currently ignored.
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

// PollComponent waits until the supplied predicate is satisfied when evaluated against the
// component identified by the supplied component reference. If there is no error, it returns (ref,
// nil) where ref is a component reference identifying the state at which the predicate was
// satisfied. It's possible that multiple state transitions (multiple notifications) occur between
// predicate checks, therefore the predicate must be monotonic: if it returns true at execution
// state transition s it must return true at all transitions t > s. It is an error if execution
// transition history is (after reloading from persistence) behind the requested ref, or if the ref
// is inconsistent with execution transition history. Thus when the predicate function is evaluated,
// it is guaranteed that the execution VT >= requestRef VT. opts are currently ignored.
// PollComponent subscribes to execution-level notifications. Suppose that an execution consists of
// one component A, and A has subcomponent B. Subscribers interested only in component B may be
// woken up unnecessarily (and thus evaluate the predicate unnecessarily) due to changes in parts of
// A that do not also belong to B.
func (e *ChasmEngine) PollComponent(
	ctx context.Context,
	requestRef chasm.ComponentRef,
	monotonicPredicate func(chasm.Context, chasm.Component) (bool, error),
	opts ...chasm.TransitionOption,
) (retRef []byte, retError error) {

	var ch <-chan struct{}
	var unsubscribe func()
	defer func() {
		if unsubscribe != nil {
			unsubscribe()
		}
	}()

	checkPredicateOrSubscribe := func() ([]byte, error) {
		_, executionLease, err := e.getExecutionLease(ctx, requestRef)
		if err != nil {
			return nil, err
		}
		defer executionLease.GetReleaseFn()(nil) //nolint:revive

		ref, err := e.predicateSatisfied(ctx, monotonicPredicate, requestRef, executionLease)
		if err != nil {
			return nil, err
		}
		if ref != nil {
			return ref, nil
		}
		// Predicate not satisfied; subscribe before releasing the lock.
		workflowKey := executionLease.GetContext().GetWorkflowKey()
		ch, unsubscribe = e.notifier.Subscribe(chasm.ExecutionKey{
			NamespaceID: workflowKey.NamespaceID,
			BusinessID:  workflowKey.WorkflowID,
			RunID:       workflowKey.RunID,
		})
		return nil, nil
	}

	ref, err := checkPredicateOrSubscribe()
	if err != nil || ref != nil {
		return ref, err
	}

	for {
		select {
		case <-ch:
			ref, err = checkPredicateOrSubscribe()
			if err != nil || ref != nil {
				return ref, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// predicateSatisfied is a helper function for PollComponent. It returns (ref, err) where ref is non-nil
// iff there's no error and predicate evaluates to true.
func (e *ChasmEngine) predicateSatisfied(
	ctx context.Context,
	predicate func(chasm.Context, chasm.Component) (bool, error),
	ref chasm.ComponentRef,
	executionLease api.WorkflowLease,
) ([]byte, error) {
	chasmTree, ok := executionLease.GetMutableState().ChasmTree().(*chasm.Node)
	if !ok {
		return nil, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			executionLease.GetMutableState().ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewContext(ctx, chasmTree)
	component, err := chasmTree.Component(chasmContext, ref)
	if err != nil {
		return nil, err
	}
	satisfied, err := predicate(chasmContext, component)
	if err != nil {
		return nil, err
	}
	if !satisfied {
		return nil, nil
	}
	return chasmContext.Ref(component)
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
	return e.createNewExecutionWithUpdate(
		ctx,
		shardContext,
		executionRef,
		archetypeID,
		newFn,
		nil,
		options,
	)
}

func (e *ChasmEngine) createNewExecutionWithUpdate(
	ctx context.Context,
	shardContext historyi.ShardContext,
	executionRef chasm.ComponentRef,
	archetypeID chasm.ArchetypeID,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
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
	if err := chasmTree.SetRootComponent(rootComponent); err != nil {
		return newExecutionParams{}, err
	}

	if updateFn != nil {
		if err = updateFn(chasmContext, rootComponent); err != nil {
			return newExecutionParams{}, err
		}
	}

	snapshot, events, err := mutableState.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
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

func currentExecutionInfoFromMutableState(ms historyi.MutableState) currentExecutionInfo {
	execState := ms.GetExecutionState()
	state, status := ms.GetWorkflowStateStatus()
	return currentExecutionInfo{
		createRequestID: execState.GetCreateRequestId(),
		CurrentWorkflowConditionFailedError: &persistence.CurrentWorkflowConditionFailedError{
			RunID:            ms.GetWorkflowKey().RunID,
			State:            state,
			Status:           status,
			LastWriteVersion: ms.GetCurrentVersion(),
			RequestIDs:       execState.GetRequestIds(),
		},
	}
}

func (e *ChasmEngine) handleExecutionConflict(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newExecutionParams newExecutionParams,
	currentRunInfo currentExecutionInfo,
	options chasm.TransitionOptions,
) (chasm.StartExecutionResult, error) {
	// Check if this a retried request using requestID.
	if _, ok := currentRunInfo.RequestIDs[options.RequestID]; ok {
		newExecutionParams.executionRef.RunID = currentRunInfo.RunID
		serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
		if err != nil {
			return chasm.StartExecutionResult{}, err
		}
		return chasm.StartExecutionResult{
			ExecutionKey: newExecutionParams.executionRef.ExecutionKey,
			ExecutionRef: serializedRef,
		}, nil
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
		return chasm.StartExecutionResult{}, serviceerror.NewNamespaceNotActive(
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
		return chasm.StartExecutionResult{}, serviceerror.NewInternal(
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
) (chasm.StartExecutionResult, error) {
	switch conflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
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
		return chasm.StartExecutionResult{}, serviceerror.NewUnimplemented("ID Conflict Policy Terminate Existing is not yet supported")
	case chasm.BusinessIDConflictPolicyUseExisting:
		existingExecutionRef := newExecutionParams.executionRef
		existingExecutionRef.RunID = currentRunInfo.RunID
		serializedRef, err := existingExecutionRef.Serialize(e.registry)
		if err != nil {
			return chasm.StartExecutionResult{}, err
		}
		return chasm.StartExecutionResult{
			ExecutionKey: existingExecutionRef.ExecutionKey,
			ExecutionRef: serializedRef,
		}, nil
	default:
		return chasm.StartExecutionResult{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID conflict policy: %v", conflictPolicy),
		)
	}
}

func (e *ChasmEngine) handleReusePolicy(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newExecutionParams newExecutionParams,
	currentRunInfo currentExecutionInfo,
	reusePolicy chasm.BusinessIDReusePolicy,
) (chasm.StartExecutionResult, error) {
	switch reusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
		// No more check needed.
		// Fallthrough to persist the new execution as current run.
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if _, ok := consts.FailedWorkflowStatuses[currentRunInfo.Status]; !ok {
			return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
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
		return chasm.StartExecutionResult{}, chasm.NewExecutionAlreadyStartedErr(
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
		return chasm.StartExecutionResult{}, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID reuse policy: %v", reusePolicy),
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
		return chasm.StartExecutionResult{}, err
	}

	serializedRef, err := newExecutionParams.executionRef.Serialize(e.registry)
	if err != nil {
		return chasm.StartExecutionResult{ExecutionKey: newExecutionParams.executionRef.ExecutionKey, Created: true}, err
	}
	return chasm.StartExecutionResult{
		ExecutionKey: newExecutionParams.executionRef.ExecutionKey,
		ExecutionRef: serializedRef,
		Created:      true,
	}, nil
}

func (e *ChasmEngine) getShardContext(
	ref chasm.ComponentRef,
) (historyi.ShardContext, error) {
	return e.shardController.GetShardByID(
		common.WorkflowIDToHistoryShard(
			ref.NamespaceID,
			ref.BusinessID,
			e.config.NumberOfShards,
		),
	)
}

// getExecutionLease returns shard context and mutable state for the chasm execution, with the lock
// held. An error is returned if the state transition specified by the component reference is
// inconsistent with mutable state transition history. If the state transition specified by the
// component reference is consistent with mutable state being stale, then mutable state is reloaded
// from persistence. The returned mutable state is guaranteed not to be stale with respect to the
// supplied component reference.
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
	if callerType == headers.CallerTypeBackgroundHigh ||
		callerType == headers.CallerTypeBackgroundLow ||
		callerType == headers.CallerTypePreemptable {
		lockPriority = locks.PriorityLow
	}

	archetypeID, err := ref.ArchetypeID(e.registry)
	if err != nil {
		return nil, nil, err
	}

	var predicateErr error
	var needReload bool
	executionLease, err := consistencyChecker.GetChasmLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState historyi.MutableState) bool {
			err := mutableState.ChasmTree().IsStale(ref)
			needReload = errors.Is(err, consts.ErrStaleState)
			if !needReload {
				// Reference itself might be stale.
				// No need to reload mutable state in this case, but request should be failed.
				predicateErr = err
			}

			// Return false (i.e. consistency predicate check failed)
			// here to indicate reload is needed.
			return !needReload
		},
		definition.NewWorkflowKey(
			ref.NamespaceID,
			ref.BusinessID,
			ref.RunID,
		),
		archetypeID,
		lockPriority,
	)
	if err != nil {
		return nil, nil, e.convertError(err, archetypeID, ref.BusinessID)
	}

	if predicateErr != nil {
		executionLease.GetReleaseFn()(nil)
		return nil, nil, predicateErr
	}

	if !needReload {
		return shardContext, executionLease, nil
	}

	// Mutable state was previously detected as stale and got reloaded,
	// do a final check to ensure mutable state is not stale after reload.
	err = executionLease.GetMutableState().ChasmTree().IsStale(ref)
	if err != nil {
		logger := log.With(shardContext.GetLogger(),
			tag.WorkflowNamespaceID(ref.NamespaceID),
			tag.WorkflowID(ref.BusinessID),
			tag.WorkflowRunID(ref.RunID),
		)

		if errors.Is(err, consts.ErrStaleState) {
			// This could happen when there's a replication lag upon force failover,
			// and caller is using the reference from the original cluster in the
			// new cluster before replication catches up.
			//
			// This could also happen due to data loss in the database,
			// but we can't really distinguish the two cases here, so always return
			// a retryable error here.
			//
			// TODO: consider adding a clusterID field to the generated token to distinguish
			// the two cases.
			logger.Warn(
				"stale state after mutable state reload, could due to force namespace failover or data loss",
				tag.Error(err),
			)

			executionLease.GetReleaseFn()(nil)
			return nil, nil, serviceerror.NewUnavailablef("stale state, please retry")
		}

		// Stale reference case is already handled above.
		executionLease.GetReleaseFn()(nil)
		return nil, nil, softassert.UnexpectedInternalErr(
			logger,
			"Unexpected stale reference in final execution staleness check",
			err,
		)
	}

	return shardContext, executionLease, nil
}

// convertError is a hook containing error conversion logic that creates more appropriate and/or
// helpful errors.
func (e *ChasmEngine) convertError(err error, archetypeID chasm.ArchetypeID, businessID string) error {
	switch {
	case errors.As(err, new(*serviceerror.NotFound)):
		displayName, ok := e.registry.ArchetypeDisplayName(archetypeID)
		if !ok {
			displayName = "execution"
		}
		return serviceerror.NewNotFoundf("%s not found for ID: %s", displayName, businessID)
	default:
		return err
	}
}

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
	"go.uber.org/fx"
)

type (
	ChasmEngine struct {
		entityCache     cache.Cache
		shardController shard.Controller
		registry        *chasm.Registry
		config          *configs.Config
		notifier        *ChasmNotifier
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

var ChasmEngineModule = fx.Options(
	fx.Provide(NewChasmNotifier),
	fx.Provide(newChasmEngine),
	fx.Provide(func(impl *ChasmEngine) chasm.Engine { return impl }),
	fx.Invoke(func(impl *ChasmEngine, shardController shard.Controller) {
		impl.SetShardController(shardController)
	}),
)

func newChasmEngine(
	entityCache cache.Cache,
	registry *chasm.Registry,
	config *configs.Config,
	notifier *ChasmNotifier,
) *ChasmEngine {
	return &ChasmEngine{
		entityCache: entityCache,
		registry:    registry,
		config:      config,
		notifier:    notifier,
	}
}

// This is for breaking fx cycle dependency.
// ChasmEngine -> ShardController -> ShardContextFactory -> HistoryEngineFactory -> QueueFactory -> ChasmEngine
func (e *ChasmEngine) SetShardController(
	shardController shard.Controller,
) {
	e.shardController = shardController
}

func (e *ChasmEngine) NotifyExecution(key chasm.EntityKey) {
	e.notifier.Notify(key)
}

func (e *ChasmEngine) NewEntity(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	opts ...chasm.TransitionOption,
) (entityKey chasm.EntityKey, newEntityRef []byte, retErr error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(entityRef)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}

	currentEntityReleaseFn, err := e.lockCurrentEntity(
		ctx,
		shardContext,
		namespace.ID(entityRef.NamespaceID),
		entityRef.BusinessID,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
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
		return chasm.EntityKey{}, nil, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newEntityParams,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	if !hasCurrentRun {
		serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
		if err != nil {
			return chasm.EntityKey{}, nil, err
		}
		return newEntityParams.entityRef.EntityKey, serializedRef, nil
	}

	return e.handleEntityConflict(
		ctx,
		shardContext,
		newEntityParams,
		currentRunInfo,
		options,
	)
}

func (e *ChasmEngine) UpdateWithNewEntity(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (newEntityKey chasm.EntityKey, newEntityRef []byte, retError error) {
	return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("UpdateWithNewEntity is not yet supported")
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
			if errors.Is(err, consts.ErrStaleState) {
				err = serviceerror.NewUnavailable("please retry")
			}
			return nil, err
		}
		if ref != nil {
			return ref, nil
		}
		// Predicate not satisfied; subscribe before releasing the lock.
		ch, unsubscribe = e.notifier.Subscribe(requestRef.EntityKey)
		return nil, nil
	}

	ref, err := checkPredicateOrSubscribe()
	if err != nil || ref != nil {
		return ref, err
	}

	for {
		select {
		case <-ch:
			ref, err := checkPredicateOrSubscribe()
			if err != nil {
				return nil, err
			}
			if ref != nil {
				return ref, nil
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

	// It is not acceptable to declare the predicate to be satisfied against execution state that is
	// behind the requested reference. However, getExecutionLease does not currently guarantee that
	// execution VT >= ref VT, therefore we call IsStale() again here and return any error (which at
	// this point must be ErrStaleState; ErrStaleReference has already been eliminated).
	err := chasmTree.IsStale(ref)
	if err != nil {
		// ErrStaleState
		// TODO(dan): this should be retryable if it is the failover version that is stale
		return nil, err
	}
	// We know now that execution VT >= ref VT

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

	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return newEntityParams{}, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
		)
	}

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
) (chasm.EntityKey, []byte, error) {
	// Check if this a retired request using requestID.
	if _, ok := currentRunInfo.RequestIDs[options.RequestID]; ok {
		newEntityParams.entityRef.EntityID = currentRunInfo.RunID
		serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
		if err != nil {
			return chasm.EntityKey{}, nil, err
		}
		return newEntityParams.entityRef.EntityKey, serializedRef, nil
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
		return chasm.EntityKey{}, nil, serviceerror.NewNamespaceNotActive(
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
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
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
) (chasm.EntityKey, []byte, error) {
	switch conflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
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
		return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("ID Conflict Policy Terminate Existing is not yet supported")
	// case chasm.BusinessIDConflictPolicyUseExisting:
	// 	return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("ID Conflict Policy Use Existing is not yet supported")
	default:
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
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
) (chasm.EntityKey, []byte, error) {
	switch reusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
		// No more check needed.
		// Fallthrough to persist the new entity as current run.
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if _, ok := consts.FailedWorkflowStatuses[currentRunInfo.Status]; !ok {
			return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
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
		return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
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
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
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
		return chasm.EntityKey{}, nil, err
	}

	serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	return newEntityParams.entityRef.EntityKey, serializedRef, nil
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

// getExecutionLease returns shard context and mutable state for the execution identified by the
// supplied component reference, with the lock held. An error is returned if the state transition
// specified by the component reference is inconsistent with mutable state transition history. If
// the state transition specified by the component reference is consistent with mutable state being
// stale, then mutable state is reloaded from persistence before returning. It does not check that
// mutable state is non-stale after reload.
// TODO(dan): if mutable state is stale after reload, return an error (retryable iff the failover
// version is stale since that is expected under some multi-cluster scenarios).
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
	if callerType == headers.CallerTypeBackgroundHigh || callerType == headers.CallerTypeBackgroundLow || callerType == headers.CallerTypePreemptable {
		lockPriority = locks.PriorityLow
	}

	archetype, err := ref.Archetype(e.registry)
	if err != nil {
		return nil, nil, err
	}

	var staleReferenceErr error
	entityLease, err := consistencyChecker.GetChasmLeaseWithConsistencyCheck(
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
		archetype,
		lockPriority,
	)
	if err == nil && staleReferenceErr != nil {
		entityLease.GetReleaseFn()(nil)
		err = staleReferenceErr
	}

	return shardContext, entityLease, err
}

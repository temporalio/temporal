package localexecution

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/versionhistory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	bridgeWorkflowPollTimeout   = common.DefaultLongPollTimeout + 10*time.Second
	bridgePollRetryInitialDelay = 100 * time.Millisecond
	bridgePollRetryMaximumDelay = time.Second
)

type WorkflowTaskPoller interface {
	PollWorkflowTaskQueue(
		ctx context.Context,
		request *workflowservice.PollWorkflowTaskQueueRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.PollWorkflowTaskQueueResponse, error)
}

type BridgeRuntimeOptions struct {
	Configuration       BridgeConfiguration
	StateStore          *BridgeStateStore
	UpstreamNamespaceID string
	LocalNamespaceID    string
	UpstreamWorkflow    WorkflowTaskPoller
	UpstreamAdmin       adminservice.AdminServiceClient
	LocalAdmin          adminservice.AdminServiceClient
}

type BridgeRuntime struct {
	configuration       BridgeConfiguration
	stateStore          *BridgeStateStore
	upstreamNamespaceID string
	localNamespaceID    string
	upstreamWorkflow    WorkflowTaskPoller
	upstreamAdmin       adminservice.AdminServiceClient
	localAdmin          adminservice.AdminServiceClient
	syncInterval        time.Duration
	workflowTypes       map[string]struct{}

	mu      sync.Mutex
	started bool
}

type managedBridgeExecution struct {
	record      BridgeExecutionRecord
	execution   *commonpb.WorkflowExecution
	replicator  *HistoryReplicator
	controller  *ExecutionStateController
	leaseExpiry time.Time
}

func NewBridgeRuntime(options BridgeRuntimeOptions) (*BridgeRuntime, error) {
	if err := options.Configuration.Validate(); err != nil {
		return nil, fmt.Errorf("validate bridge configuration: %w", err)
	}
	if options.StateStore == nil {
		return nil, errors.New("bridge state store is required")
	}
	if options.UpstreamNamespaceID == "" || options.LocalNamespaceID == "" {
		return nil, errors.New("upstream and local namespace IDs are required")
	}
	if options.UpstreamWorkflow == nil {
		return nil, errors.New("upstream workflow client is required")
	}
	if options.UpstreamAdmin == nil || options.LocalAdmin == nil {
		return nil, errors.New("upstream and local admin clients are required")
	}
	if options.Configuration.Options.SyncIntervalMilliseconds > int64(math.MaxInt64/time.Millisecond) {
		return nil, errors.New("sync interval is outside the supported duration range")
	}
	configuration := cloneBridgeConfiguration(options.Configuration)
	syncInterval := time.Duration(configuration.Options.SyncIntervalMilliseconds) * time.Millisecond
	workflowTypes := make(map[string]struct{}, len(configuration.Registrations.WorkflowTypes))
	for _, workflowType := range configuration.Registrations.WorkflowTypes {
		workflowTypes[workflowType] = struct{}{}
	}
	return &BridgeRuntime{
		configuration:       configuration,
		stateStore:          options.StateStore,
		upstreamNamespaceID: options.UpstreamNamespaceID,
		localNamespaceID:    options.LocalNamespaceID,
		upstreamWorkflow:    options.UpstreamWorkflow,
		upstreamAdmin:       options.UpstreamAdmin,
		localAdmin:          options.LocalAdmin,
		syncInterval:        syncInterval,
		workflowTypes:       workflowTypes,
	}, nil
}

func cloneBridgeConfiguration(configuration BridgeConfiguration) BridgeConfiguration {
	configuration.Registrations.WorkflowTypes = append(
		[]string(nil),
		configuration.Registrations.WorkflowTypes...,
	)
	configuration.Registrations.ActivityTypes = append(
		[]string(nil),
		configuration.Registrations.ActivityTypes...,
	)
	if configuration.Upstream.Headers != nil {
		headers := make(map[string]string, len(configuration.Upstream.Headers))
		for name, value := range configuration.Upstream.Headers {
			headers[name] = value
		}
		configuration.Upstream.Headers = headers
	}
	if configuration.Upstream.TLS != nil {
		tls := *configuration.Upstream.TLS
		configuration.Upstream.TLS = &tls
	}
	return configuration
}

func (r *BridgeRuntime) Start(ctx context.Context) (<-chan error, error) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return nil, errors.New("bridge runtime is already started")
	}
	r.started = true
	r.mu.Unlock()

	runtimeContext, cancelRuntime := context.WithCancelCause(ctx)
	var executionGroup sync.WaitGroup
	records, err := r.stateStore.LoadExecutions()
	if err != nil {
		cancelRuntime(err)
		return nil, fmt.Errorf("load bridge executions: %w", err)
	}
	for _, record := range records {
		managed, err := r.recoverExecution(runtimeContext, record)
		if err != nil {
			cancelRuntime(err)
			executionGroup.Wait()
			return nil, fmt.Errorf("recover local execution %s/%s: %w", record.WorkflowID, record.RunID, err)
		}
		if managed != nil {
			r.startSynchronization(runtimeContext, cancelRuntime, &executionGroup, managed)
		}
	}

	done := make(chan error, 1)
	go func() {
		if err := r.runAcquisitionLoop(runtimeContext, cancelRuntime, &executionGroup); err != nil {
			cancelRuntime(err)
		}
		executionGroup.Wait()
		cause := context.Cause(runtimeContext)
		if ctx.Err() != nil || errors.Is(cause, context.Canceled) {
			done <- nil
		} else {
			done <- cause
		}
		close(done)
	}()
	return done, nil
}

func (r *BridgeRuntime) runAcquisitionLoop(
	ctx context.Context,
	cancelRuntime context.CancelCauseFunc,
	executionGroup *sync.WaitGroup,
) error {
	if len(r.workflowTypes) == 0 {
		<-ctx.Done()
		return nil
	}
	retryDelay := bridgePollRetryInitialDelay
	for {
		response, err := r.pollUpstream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if transientPollError(err) {
				if err := waitForDelay(ctx, retryDelay); err != nil {
					return nil
				}
				retryDelay = min(retryDelay*2, bridgePollRetryMaximumDelay)
				continue
			}
			return fmt.Errorf("poll upstream workflow task queue: %w", err)
		}
		retryDelay = bridgePollRetryInitialDelay
		managed, err := r.handleAcquisitionResponse(ctx, response)
		if err != nil {
			return err
		}
		if managed != nil {
			r.startSynchronization(ctx, cancelRuntime, executionGroup, managed)
		}
	}
}

func (r *BridgeRuntime) handleAcquisitionResponse(
	ctx context.Context,
	response *workflowservice.PollWorkflowTaskQueueResponse,
) (*managedBridgeExecution, error) {
	if response.GetLocalExecutionInfo() == nil && response.GetWorkflowExecution() == nil {
		return nil, nil
	}
	if err := r.validateAcquisition(response); err != nil {
		return nil, err
	}
	if _, registered := r.workflowTypes[response.GetWorkflowType().GetName()]; registered {
		return r.adoptExecution(ctx, response)
	}
	if err := r.releaseAcquisition(ctx, response); err != nil {
		return nil, fmt.Errorf("release unregistered workflow type: %w", err)
	}
	return nil, nil
}

func (r *BridgeRuntime) pollUpstream(
	ctx context.Context,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	pollContext, cancelPoll := context.WithTimeout(ctx, bridgeWorkflowPollTimeout)
	defer cancelPoll()
	return r.upstreamWorkflow.PollWorkflowTaskQueue(
		pollContext,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: r.configuration.Namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: r.configuration.Registrations.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: r.stateStore.LocalServerID(),
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          r.stateStore.LocalServerID(),
				ProtocolVersion:        ProtocolVersion,
				SyncInterval:           durationpb.New(r.syncInterval),
				RequestedLeaseDuration: durationpb.New(3 * r.syncInterval),
			},
		},
	)
}

func transientPollError(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Unavailable:
		return true
	default:
		return false
	}
}

func waitForDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func (r *BridgeRuntime) validateAcquisition(response *workflowservice.PollWorkflowTaskQueueResponse) error {
	info := response.GetLocalExecutionInfo()
	if info == nil {
		return errors.New("upstream returned an ordinary workflow task to a local execution poll")
	}
	if len(response.GetTaskToken()) != 0 || response.GetStartedEventId() != 0 {
		return errors.New("local execution acquisition started the upstream workflow task")
	}
	if response.GetWorkflowExecution().GetWorkflowId() == "" || response.GetWorkflowExecution().GetRunId() == "" {
		return errors.New("local execution acquisition returned no execution identity")
	}
	if response.GetWorkflowType().GetName() == "" {
		return errors.New("local execution acquisition returned no workflow type")
	}
	if len(info.GetOwnershipToken()) != sha256.Size || info.GetFencingEpoch() <= 0 {
		return errors.New("local execution acquisition returned invalid ownership")
	}
	if info.GetLastSynchronizedEventId() < common.FirstEventID {
		return errors.New("local execution acquisition returned an invalid cursor")
	}
	if info.GetLeaseExpirationTime() == nil || info.GetLeaseExpirationTime().CheckValid() != nil {
		return errors.New("local execution acquisition returned an invalid lease expiration")
	}
	return nil
}

func (r *BridgeRuntime) adoptExecution(
	ctx context.Context,
	acquisition *workflowservice.PollWorkflowTaskQueueResponse,
) (*managedBridgeExecution, error) {
	info := acquisition.GetLocalExecutionInfo()
	execution := acquisition.GetWorkflowExecution()
	record := BridgeExecutionRecord{
		Phase:                        BridgeExecutionPhaseImporting,
		Namespace:                    r.configuration.Namespace,
		NamespaceID:                  r.upstreamNamespaceID,
		WorkflowID:                   execution.GetWorkflowId(),
		RunID:                        execution.GetRunId(),
		OwnershipToken:               append([]byte(nil), info.GetOwnershipToken()...),
		FencingEpoch:                 info.GetFencingEpoch(),
		LeaseExpiration:              info.GetLeaseExpirationTime().AsTime(),
		SyncIntervalMilliseconds:     r.configuration.Options.SyncIntervalMilliseconds,
		LastSynchronizedEventID:      info.GetLastSynchronizedEventId(),
		LastSynchronizedEventVersion: info.GetLastSynchronizedEventVersion(),
	}
	if err := r.stateStore.SaveExecution(record); err != nil {
		return nil, fmt.Errorf("persist acquired execution: %w", err)
	}

	baseline, err := r.importBaseline(ctx, execution)
	if err != nil {
		return nil, err
	}
	if baseline.LastEventID != record.LastSynchronizedEventID ||
		baseline.LastEventVersion != record.LastSynchronizedEventVersion {
		return nil, fmt.Errorf(
			"imported baseline cursor %d/%d does not match acquired cursor %d/%d",
			baseline.LastEventID,
			baseline.LastEventVersion,
			record.LastSynchronizedEventID,
			record.LastSynchronizedEventVersion,
		)
	}
	managed, err := r.newManagedExecution(record)
	if err != nil {
		return nil, err
	}
	if err := managed.controller.Update(
		ctx,
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
	); err != nil {
		return nil, fmt.Errorf("establish imported execution owner: %w", err)
	}
	record.Phase = BridgeExecutionPhaseReady
	if err := r.stateStore.SaveExecution(record); err != nil {
		return nil, fmt.Errorf("commit imported execution state: %w", err)
	}
	managed.record = record
	if err := managed.controller.Update(
		ctx,
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE,
	); err != nil {
		return nil, fmt.Errorf("make imported execution runnable: %w", err)
	}
	return managed, nil
}

func (r *BridgeRuntime) recoverExecution(
	ctx context.Context,
	record BridgeExecutionRecord,
) (*managedBridgeExecution, error) {
	if record.Namespace != r.configuration.Namespace || record.NamespaceID != r.upstreamNamespaceID {
		return nil, errors.New("persisted execution belongs to a different upstream namespace")
	}
	execution := &commonpb.WorkflowExecution{WorkflowId: record.WorkflowID, RunId: record.RunID}
	exists, err := r.localExecutionExists(ctx, execution)
	if err != nil {
		return nil, err
	}
	if time.Until(record.LeaseExpiration) <= 0 {
		return nil, r.discardExpiredExecution(ctx, record, execution, exists)
	}
	if err := r.ensureRecoveredBaseline(ctx, record, execution, exists); err != nil {
		return nil, err
	}

	managed, err := r.newManagedExecution(record)
	if err != nil {
		return nil, err
	}
	if err := managed.controller.Update(
		ctx,
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
	); err != nil {
		return nil, fmt.Errorf("pause recovered execution: %w", err)
	}
	record.Phase = BridgeExecutionPhaseReady
	if err := r.stateStore.SaveExecution(record); err != nil {
		return nil, fmt.Errorf("commit recovered execution state: %w", err)
	}
	managed.record = record
	return r.revalidateRecoveredExecution(ctx, managed)
}

func (r *BridgeRuntime) discardExpiredExecution(
	ctx context.Context,
	record BridgeExecutionRecord,
	execution *commonpb.WorkflowExecution,
	exists bool,
) error {
	if exists {
		managed, err := r.newManagedExecution(record)
		if err != nil {
			return err
		}
		if err := managed.controller.Update(
			ctx,
			execution,
			adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST,
		); err != nil {
			return fmt.Errorf("invalidate expired local execution: %w", err)
		}
	}
	return r.stateStore.DeleteExecution(record.Namespace, record.WorkflowID, record.RunID)
}

func (r *BridgeRuntime) ensureRecoveredBaseline(
	ctx context.Context,
	record BridgeExecutionRecord,
	execution *commonpb.WorkflowExecution,
	exists bool,
) error {
	if exists {
		return nil
	}
	if record.Phase != BridgeExecutionPhaseImporting {
		return errors.New("ready execution is absent from the local database")
	}
	baseline, err := r.importBaseline(ctx, execution)
	if err != nil {
		return err
	}
	if baseline.LastEventID != record.LastSynchronizedEventID ||
		baseline.LastEventVersion != record.LastSynchronizedEventVersion {
		return errors.New("recovered baseline does not match its persisted cursor")
	}
	return nil
}

func (r *BridgeRuntime) revalidateRecoveredExecution(
	ctx context.Context,
	managed *managedBridgeExecution,
) (*managedBridgeExecution, error) {
	syncContext, cancelSync := context.WithDeadline(ctx, managed.record.LeaseExpiration)
	result, err := managed.replicator.Sync(syncContext, managed.execution)
	cancelSync()
	if err != nil {
		if ownershipLostError(err) {
			ownershipErr := markOwnershipLost(ctx, managed.controller, managed.execution, err)
			if !errors.Is(ownershipErr, ErrLocalExecutionOwnershipLost) {
				return nil, ownershipErr
			}
			if err := r.stateStore.DeleteExecution(
				managed.record.Namespace,
				managed.record.WorkflowID,
				managed.record.RunID,
			); err != nil {
				return nil, err
			}
			return nil, nil
		}
		return nil, fmt.Errorf("revalidate recovered execution: %w", err)
	}
	if result.Released {
		if err := r.stateStore.DeleteExecution(
			managed.record.Namespace,
			managed.record.WorkflowID,
			managed.record.RunID,
		); err != nil {
			return nil, err
		}
		return nil, nil
	}
	managed.record.LastSynchronizedEventID = result.LastEventID
	managed.record.LastSynchronizedEventVersion = result.LastEventVersion
	managed.record.LeaseExpiration = result.LeaseExpiration
	if err := r.stateStore.SaveExecution(managed.record); err != nil {
		return nil, fmt.Errorf("persist recovered synchronization: %w", err)
	}
	managed.leaseExpiry = result.LeaseExpiration
	if err := managed.controller.Update(
		ctx,
		managed.execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE,
	); err != nil {
		return nil, fmt.Errorf("resume recovered execution: %w", err)
	}
	return managed, nil
}

func (r *BridgeRuntime) importBaseline(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (SyncResult, error) {
	importer, err := NewBaselineImporter(
		HistoryEndpoint{NamespaceID: r.upstreamNamespaceID, AdminClient: r.upstreamAdmin},
		HistoryEndpoint{Namespace: r.configuration.Namespace, AdminClient: r.localAdmin},
	)
	if err != nil {
		return SyncResult{}, err
	}
	result, err := importer.Import(ctx, execution)
	if err != nil {
		return SyncResult{}, fmt.Errorf("import acquired baseline: %w", err)
	}
	return result, nil
}

func (r *BridgeRuntime) newManagedExecution(
	record BridgeExecutionRecord,
) (*managedBridgeExecution, error) {
	controller, err := NewExecutionStateController(
		record.Namespace,
		r.stateStore.LocalServerID(),
		record.FencingEpoch,
		r.localAdmin,
	)
	if err != nil {
		return nil, err
	}
	replicator, err := NewHistoryReplicator(
		HistoryEndpoint{NamespaceID: r.localNamespaceID, AdminClient: r.localAdmin},
		ReplicationTarget{
			Namespace:      record.Namespace,
			LocalServerID:  r.stateStore.LocalServerID(),
			OwnershipToken: record.OwnershipToken,
			FencingEpoch:   record.FencingEpoch,
			AdminClient:    r.upstreamAdmin,
			Registrations:  &r.configuration.Registrations,
		},
		SyncCursor{
			EventID: record.LastSynchronizedEventID,
			Version: record.LastSynchronizedEventVersion,
		},
	)
	if err != nil {
		return nil, err
	}
	return &managedBridgeExecution{
		record:      record,
		execution:   &commonpb.WorkflowExecution{WorkflowId: record.WorkflowID, RunId: record.RunID},
		replicator:  replicator,
		controller:  controller,
		leaseExpiry: record.LeaseExpiration,
	}, nil
}

func (r *BridgeRuntime) startSynchronization(
	ctx context.Context,
	cancelRuntime context.CancelCauseFunc,
	executionGroup *sync.WaitGroup,
	managed *managedBridgeExecution,
) {
	executionGroup.Add(1)
	go func() {
		defer executionGroup.Done()
		released := false
		err := managed.replicator.RunControlled(
			ctx,
			managed.execution,
			SynchronizationLoopOptions{
				Interval:        r.syncInterval,
				LeaseExpiration: managed.leaseExpiry,
				StateController: managed.controller,
			},
			func(result SyncResult) error {
				if result.Released {
					released = true
					return r.stateStore.DeleteExecution(
						managed.record.Namespace,
						managed.record.WorkflowID,
						managed.record.RunID,
					)
				}
				managed.record.LastSynchronizedEventID = result.LastEventID
				managed.record.LastSynchronizedEventVersion = result.LastEventVersion
				managed.record.LeaseExpiration = result.LeaseExpiration
				return r.stateStore.SaveExecution(managed.record)
			},
		)
		if errors.Is(err, ErrLocalExecutionOwnershipLost) {
			if deleteErr := r.stateStore.DeleteExecution(
				managed.record.Namespace,
				managed.record.WorkflowID,
				managed.record.RunID,
			); deleteErr != nil {
				cancelRuntime(fmt.Errorf("remove lost execution state: %w", deleteErr))
			}
			return
		}
		if err != nil {
			cancelRuntime(fmt.Errorf("synchronize local execution %s/%s: %w", managed.record.WorkflowID, managed.record.RunID, err))
			return
		}
		if !released && ctx.Err() == nil {
			cancelRuntime(errors.New("local execution synchronization stopped before release"))
		}
	}()
}

func (r *BridgeRuntime) localExecutionExists(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (bool, error) {
	_, err := r.localAdmin.GetWorkflowExecutionRawHistoryV2(
		ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId:     r.localNamespaceID,
			Execution:       execution,
			EndEventId:      common.EndEventID,
			MaximumPageSize: 1,
		},
	)
	if err == nil {
		return true, nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, fmt.Errorf("inspect local execution: %w", err)
}

func (r *BridgeRuntime) releaseAcquisition(
	ctx context.Context,
	acquisition *workflowservice.PollWorkflowTaskQueueResponse,
) error {
	execution := acquisition.GetWorkflowExecution()
	history, err := r.upstreamAdmin.GetWorkflowExecutionRawHistoryV2(
		ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId:     r.upstreamNamespaceID,
			Execution:       execution,
			EndEventId:      common.EndEventID,
			MaximumPageSize: 1,
		},
	)
	if err != nil {
		return fmt.Errorf("read upstream cursor: %w", err)
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(history.GetVersionHistory())
	if err != nil {
		return fmt.Errorf("read upstream version history: %w", err)
	}
	info := acquisition.GetLocalExecutionInfo()
	if lastItem.GetEventId() != info.GetLastSynchronizedEventId() ||
		lastItem.GetVersion() != info.GetLastSynchronizedEventVersion() {
		return errors.New("upstream advanced after local execution acquisition")
	}
	response, err := r.upstreamAdmin.SyncLocalExecution(ctx, &adminservice.SyncLocalExecutionRequest{
		Namespace:            r.configuration.Namespace,
		Execution:            execution,
		ProtocolVersion:      ProtocolVersion,
		LocalServerId:        r.stateStore.LocalServerID(),
		SyncId:               uuid.NewString(),
		PreviousEventId:      lastItem.GetEventId(),
		PreviousEventVersion: lastItem.GetVersion(),
		NewEventId:           lastItem.GetEventId(),
		NewEventVersion:      lastItem.GetVersion(),
		VersionHistory:       history.GetVersionHistory(),
		OwnershipToken:       info.GetOwnershipToken(),
		FencingEpoch:         info.GetFencingEpoch(),
		Release:              true,
	})
	if err != nil {
		return err
	}
	if response.GetLeaseExpirationTime() != nil ||
		response.GetAcknowledgedEventId() != lastItem.GetEventId() ||
		response.GetAcknowledgedEventVersion() != lastItem.GetVersion() {
		return errors.New("upstream did not acknowledge acquisition release")
	}
	return nil
}

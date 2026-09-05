package localexecution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultMaximumPageSize = 100

var ErrLocalExecutionOwnershipLost = errors.New("local execution ownership lost")

type HistoryEndpoint struct {
	Namespace   string
	NamespaceID string
	AdminClient adminservice.AdminServiceClient
}

type BaselineImporter struct {
	source          HistoryEndpoint
	target          HistoryEndpoint
	maximumPageSize int32
}

type SyncResult struct {
	HistoryBatches   int
	LastEventID      int64
	LastEventVersion int64
	Released         bool
	LeaseExpiration  time.Time
}

type SyncCursor struct {
	EventID int64
	Version int64
}

type ReplicationTarget struct {
	Namespace      string
	LocalServerID  string
	OwnershipToken []byte
	FencingEpoch   int64
	AdminClient    adminservice.AdminServiceClient
	Registrations  *WorkerRegistrationManifest
}

type HistoryReplicator struct {
	source          HistoryEndpoint
	target          ReplicationTarget
	maximumPageSize int32
	cursor          SyncCursor
	eventSerializer serialization.Serializer
	pendingRequest  *adminservice.SyncLocalExecutionRequest
	registrations   *WorkerRegistrationManifest
	activityTypes   map[string]struct{}
}

type SynchronizationLoopOptions struct {
	Interval          time.Duration
	LeaseExpiration   time.Time
	StateController   *ExecutionStateController
	RetryInitialDelay time.Duration
	RetryMaximumDelay time.Duration
}

type ExecutionStateController struct {
	namespace     string
	localServerID string
	fencingEpoch  int64
	adminClient   adminservice.AdminServiceClient
}

const localExecutionBoundaryPollInterval = 100 * time.Millisecond

func NewBaselineImporter(source HistoryEndpoint, target HistoryEndpoint) (*BaselineImporter, error) {
	if source.NamespaceID == "" {
		return nil, errors.New("source namespace ID is required")
	}
	if source.AdminClient == nil {
		return nil, errors.New("source admin client is required")
	}
	if target.Namespace == "" {
		return nil, errors.New("target namespace is required")
	}
	if target.AdminClient == nil {
		return nil, errors.New("target admin client is required")
	}

	return &BaselineImporter{
		source:          source,
		target:          target,
		maximumPageSize: defaultMaximumPageSize,
	}, nil
}

func (s *BaselineImporter) Import(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (SyncResult, error) {
	if execution.GetWorkflowId() == "" || execution.GetRunId() == "" {
		return SyncResult{}, errors.New("workflow ID and run ID are required")
	}

	var result SyncResult
	var sourceToken []byte
	var importToken []byte
	var lastVersionHistory *adminservice.GetWorkflowExecutionRawHistoryV2Response

	for {
		response, err := s.source.AdminClient.GetWorkflowExecutionRawHistoryV2(
			ctx,
			&adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId:     s.source.NamespaceID,
				Execution:       execution,
				EndEventId:      common.EndEventID,
				MaximumPageSize: s.maximumPageSize,
				NextPageToken:   sourceToken,
			},
		)
		if err != nil {
			return SyncResult{}, fmt.Errorf("read source history: %w", err)
		}

		lastVersionHistory = response
		if len(response.HistoryBatches) > 0 {
			importResponse, err := s.target.AdminClient.ImportWorkflowExecution(
				ctx,
				&adminservice.ImportWorkflowExecutionRequest{
					Namespace:      s.target.Namespace,
					Execution:      execution,
					HistoryBatches: response.HistoryBatches,
					VersionHistory: response.VersionHistory,
					Token:          importToken,
				},
			)
			if err != nil {
				return SyncResult{}, fmt.Errorf("import history batches: %w", err)
			}
			importToken = importResponse.Token
			result.HistoryBatches += len(response.HistoryBatches)
		}

		if len(response.NextPageToken) == 0 {
			break
		}
		sourceToken = response.NextPageToken
	}

	if lastVersionHistory == nil || lastVersionHistory.VersionHistory == nil {
		return SyncResult{}, errors.New("source returned no version history")
	}
	if result.HistoryBatches == 0 {
		return SyncResult{}, errors.New("source returned no history batches")
	}

	commitResponse, err := s.target.AdminClient.ImportWorkflowExecution(
		ctx,
		&adminservice.ImportWorkflowExecutionRequest{
			Namespace:      s.target.Namespace,
			Execution:      execution,
			VersionHistory: lastVersionHistory.VersionHistory,
			Token:          importToken,
		},
	)
	if err != nil {
		return SyncResult{}, fmt.Errorf("commit imported history: %w", err)
	}
	if len(commitResponse.Token) != 0 {
		return SyncResult{}, errors.New("history import commit returned a continuation token")
	}

	items := lastVersionHistory.VersionHistory.Items
	if len(items) > 0 {
		result.LastEventID = items[len(items)-1].EventId
		result.LastEventVersion = items[len(items)-1].Version
	}
	return result, nil
}

func NewHistoryReplicator(
	source HistoryEndpoint,
	target ReplicationTarget,
	cursor SyncCursor,
) (*HistoryReplicator, error) {
	if source.NamespaceID == "" {
		return nil, errors.New("source namespace ID is required")
	}
	if source.AdminClient == nil {
		return nil, errors.New("source admin client is required")
	}
	if target.Namespace == "" {
		return nil, errors.New("target namespace is required")
	}
	if target.LocalServerID == "" {
		return nil, errors.New("local server ID is required")
	}
	if target.AdminClient == nil {
		return nil, errors.New("target admin client is required")
	}
	if cursor.EventID < common.FirstEventID {
		return nil, errors.New("source cursor must identify an existing event")
	}
	activityTypes := make(map[string]struct{})
	if target.Registrations != nil {
		if target.Registrations.TaskQueue == "" {
			return nil, errors.New("registration task queue is required")
		}
		if err := validateRegistrationTypes("activity", target.Registrations.ActivityTypes); err != nil {
			return nil, err
		}
		for _, activityType := range target.Registrations.ActivityTypes {
			activityTypes[activityType] = struct{}{}
		}
	}

	return &HistoryReplicator{
		source:          source,
		target:          target,
		maximumPageSize: defaultMaximumPageSize,
		cursor:          cursor,
		eventSerializer: serialization.NewSerializer(),
		registrations:   target.Registrations,
		activityTypes:   activityTypes,
	}, nil
}

func NewExecutionStateController(
	namespace string,
	localServerID string,
	fencingEpoch int64,
	adminClient adminservice.AdminServiceClient,
) (*ExecutionStateController, error) {
	if namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if localServerID == "" {
		return nil, errors.New("local server ID is required")
	}
	if fencingEpoch <= 0 {
		return nil, errors.New("fencing epoch must be positive")
	}
	if adminClient == nil {
		return nil, errors.New("admin client is required")
	}
	return &ExecutionStateController{
		namespace:     namespace,
		localServerID: localServerID,
		fencingEpoch:  fencingEpoch,
		adminClient:   adminClient,
	}, nil
}

func (c *ExecutionStateController) Update(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
	state adminservice.UpdateLocalExecutionStateRequest_State,
) error {
	_, err := c.adminClient.UpdateLocalExecutionState(ctx, &adminservice.UpdateLocalExecutionStateRequest{
		Namespace:     c.namespace,
		Execution:     execution,
		LocalServerId: c.localServerID,
		FencingEpoch:  c.fencingEpoch,
		State:         state,
	})
	return err
}

func (c *ExecutionStateController) State(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (persistencespb.LocalExecutionInfo_BridgeState, error) {
	response, err := c.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: c.namespace,
		Execution: execution,
	})
	if err != nil {
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_UNSPECIFIED, err
	}
	mutableState := response.GetCacheMutableState()
	if mutableState == nil {
		mutableState = response.GetDatabaseMutableState()
	}
	if mutableState == nil {
		return persistencespb.LocalExecutionInfo_BRIDGE_STATE_UNSPECIFIED,
			errors.New("local server returned no mutable state")
	}
	return mutableState.GetExecutionInfo().GetLocalExecutionInfo().GetBridgeState(), nil
}

func (r *HistoryReplicator) Sync(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (SyncResult, error) {
	if execution.GetWorkflowId() == "" || execution.GetRunId() == "" {
		return SyncResult{}, errors.New("workflow ID and run ID are required")
	}

	if r.pendingRequest == nil {
		request, err := r.prepareSyncRequest(ctx, execution)
		if err != nil {
			return SyncResult{}, err
		}
		r.pendingRequest = request
	}
	request := r.pendingRequest
	response, err := r.target.AdminClient.SyncLocalExecution(ctx, request)
	if err != nil {
		return SyncResult{}, fmt.Errorf("sync local execution: %w", err)
	}
	if response.GetSyncId() != request.GetSyncId() ||
		response.GetAcknowledgedEventId() != request.GetNewEventId() ||
		response.GetAcknowledgedEventVersion() != request.GetNewEventVersion() {
		return SyncResult{}, errors.New("sync response did not acknowledge the requested cursor")
	}

	result := SyncResult{
		HistoryBatches:   len(request.GetHistoryBatches()),
		LastEventID:      request.GetNewEventId(),
		LastEventVersion: request.GetNewEventVersion(),
		Released:         response.GetLeaseExpirationTime() == nil && request.GetRelease(),
	}
	if (response.GetLeaseExpirationTime() == nil) != request.GetRelease() {
		return SyncResult{}, errors.New("sync response lease state did not match the release request")
	}
	if response.GetLeaseExpirationTime() != nil {
		if err := response.GetLeaseExpirationTime().CheckValid(); err != nil {
			return SyncResult{}, errors.New("sync response returned an invalid lease expiration")
		}
		result.LeaseExpiration = response.GetLeaseExpirationTime().AsTime()
	}
	r.cursor = SyncCursor{EventID: request.GetNewEventId(), Version: request.GetNewEventVersion()}
	r.pendingRequest = nil
	return result, nil
}

func (r *HistoryReplicator) prepareSyncRequest(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (*adminservice.SyncLocalExecutionRequest, error) {
	var sourceToken []byte
	var historyBatches []*commonpb.DataBlob
	var sourceVersionHistory *adminservice.GetWorkflowExecutionRawHistoryV2Response

	for {
		response, err := r.source.AdminClient.GetWorkflowExecutionRawHistoryV2(
			ctx,
			&adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId:       r.source.NamespaceID,
				Execution:         execution,
				StartEventId:      r.cursor.EventID,
				StartEventVersion: r.cursor.Version,
				EndEventId:        common.EndEventID,
				MaximumPageSize:   r.maximumPageSize,
				NextPageToken:     sourceToken,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("read source history delta: %w", err)
		}

		historyBatches = append(historyBatches, response.GetHistoryBatches()...)
		sourceVersionHistory = response
		if len(response.NextPageToken) == 0 {
			break
		}
		sourceToken = response.NextPageToken
	}

	if sourceVersionHistory == nil || sourceVersionHistory.GetVersionHistory() == nil {
		return nil, errors.New("source returned no version history")
	}
	historyBatches, err := r.historyAfterCursor(historyBatches)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(sourceVersionHistory.GetVersionHistory())
	if err != nil {
		return nil, fmt.Errorf("read source history cursor: %w", err)
	}
	release := r.historyRequiresUpstream(historyBatches)
	syncID := uuid.NewString()
	return &adminservice.SyncLocalExecutionRequest{
		Namespace:            r.target.Namespace,
		Execution:            execution,
		ProtocolVersion:      ProtocolVersion,
		LocalServerId:        r.target.LocalServerID,
		SyncId:               syncID,
		PreviousEventId:      r.cursor.EventID,
		PreviousEventVersion: r.cursor.Version,
		NewEventId:           lastItem.GetEventId(),
		NewEventVersion:      lastItem.GetVersion(),
		HistoryBatches:       historyBatches,
		VersionHistory:       sourceVersionHistory.GetVersionHistory(),
		OwnershipToken:       r.target.OwnershipToken,
		FencingEpoch:         r.target.FencingEpoch,
		Release:              release,
	}, nil
}

// historyAfterCursor removes any history prefix returned by the raw-history API.
// In particular, a local namespace uses version zero, which that API cannot
// distinguish from an omitted StartEventVersion field.
func (r *HistoryReplicator) historyAfterCursor(
	historyBatches []*commonpb.DataBlob,
) ([]*commonpb.DataBlob, error) {
	delta := make([]*commonpb.DataBlob, 0, len(historyBatches))
	for _, batch := range historyBatches {
		events, err := r.eventSerializer.DeserializeEvents(batch)
		if err != nil {
			return nil, fmt.Errorf("decode source history delta: %w", err)
		}
		firstNewEvent := 0
		for firstNewEvent < len(events) && events[firstNewEvent].GetEventId() <= r.cursor.EventID {
			firstNewEvent++
		}
		if firstNewEvent == len(events) {
			continue
		}
		if firstNewEvent == 0 {
			delta = append(delta, batch)
			continue
		}
		trimmedBatch, err := r.eventSerializer.SerializeEvents(events[firstNewEvent:])
		if err != nil {
			return nil, fmt.Errorf("encode source history delta: %w", err)
		}
		delta = append(delta, trimmedBatch)
	}
	return delta, nil
}

func (r *HistoryReplicator) historyClosesWorkflow(historyBatches []*commonpb.DataBlob) bool {
	if len(historyBatches) == 0 {
		return false
	}
	events, err := r.eventSerializer.DeserializeEvents(historyBatches[len(historyBatches)-1])
	if err != nil || len(events) == 0 {
		return false
	}
	switch events[len(events)-1].GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return true
	default:
		return false
	}
}

func (r *HistoryReplicator) historyRequiresUpstream(historyBatches []*commonpb.DataBlob) bool {
	for _, batch := range historyBatches {
		events, err := r.eventSerializer.DeserializeEvents(batch)
		if err != nil {
			return true
		}
		for _, event := range events {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
				if r.activityRequiresUpstream(event) {
					return true
				}
			case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
				enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
				enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
				return true
			default:
			}
		}
	}
	return r.historyClosesWorkflow(historyBatches)
}

func (r *HistoryReplicator) activityRequiresUpstream(event *historypb.HistoryEvent) bool {
	if r.registrations == nil {
		return false
	}
	attributes := event.GetActivityTaskScheduledEventAttributes()
	if attributes == nil || attributes.GetActivityType().GetName() == "" {
		return true
	}
	if attributes.GetTaskQueue().GetName() != r.registrations.TaskQueue {
		return true
	}
	_, registered := r.activityTypes[attributes.GetActivityType().GetName()]
	return !registered
}

func (r *HistoryReplicator) Run(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
	interval time.Duration,
	afterSync func(SyncResult),
) error {
	if interval <= 0 {
		return errors.New("sync interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			result, err := r.Sync(ctx, execution)
			if err != nil {
				return err
			}
			if afterSync != nil {
				afterSync(result)
			}
			if result.Released {
				<-ctx.Done()
				return nil
			}
		}
	}
}

func (r *HistoryReplicator) RunControlled(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
	options SynchronizationLoopOptions,
	afterSync func(SyncResult) error,
) error {
	if err := normalizeSynchronizationLoopOptions(&options); err != nil {
		return err
	}

	leaseExpiration := options.LeaseExpiration
	for {
		if err := waitForSynchronizationBoundary(
			ctx,
			execution,
			leaseExpiration.Add(-2*options.Interval),
			options.StateController,
		); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("observe local synchronization boundary: %w", err)
		}
		if err := options.StateController.Update(
			ctx,
			execution,
			adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
		); err != nil {
			return fmt.Errorf("pause local execution: %w", err)
		}

		result, err := r.syncWhilePaused(ctx, execution, leaseExpiration, options)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if afterSync != nil {
			if err := afterSync(result); err != nil {
				return fmt.Errorf("record synchronization result: %w", err)
			}
		}
		if result.Released {
			return nil
		}
		if result.LeaseExpiration.IsZero() {
			return errors.New("ownership-retaining sync returned no lease expiration")
		}
		leaseExpiration = result.LeaseExpiration
		if err := options.StateController.Update(
			ctx,
			execution,
			adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE,
		); err != nil {
			return fmt.Errorf("resume local execution: %w", err)
		}
	}
}

func waitForSynchronizationBoundary(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
	deadline time.Time,
	controller *ExecutionStateController,
) error {
	deadlineTimer := time.NewTimer(max(time.Until(deadline), 0))
	defer deadlineTimer.Stop()
	boundaryTicker := time.NewTicker(localExecutionBoundaryPollInterval)
	defer boundaryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-deadlineTimer.C:
			return nil
		case <-boundaryTicker.C:
			state, err := controller.State(ctx, execution)
			if err != nil {
				return err
			}
			switch state {
			case persistencespb.LocalExecutionInfo_BRIDGE_STATE_PAUSED:
				return nil
			case persistencespb.LocalExecutionInfo_BRIDGE_STATE_OWNERSHIP_LOST:
				return ErrLocalExecutionOwnershipLost
			default:
			}
		}
	}
}

func normalizeSynchronizationLoopOptions(options *SynchronizationLoopOptions) error {
	if options.Interval <= 0 {
		return errors.New("sync interval must be positive")
	}
	if options.LeaseExpiration.IsZero() {
		return errors.New("lease expiration is required")
	}
	if options.StateController == nil {
		return errors.New("state controller is required")
	}
	if options.RetryInitialDelay <= 0 {
		options.RetryInitialDelay = 100 * time.Millisecond
	}
	if options.RetryMaximumDelay <= 0 {
		options.RetryMaximumDelay = time.Second
	}
	return nil
}

func (r *HistoryReplicator) syncWhilePaused(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
	leaseExpiration time.Time,
	options SynchronizationLoopOptions,
) (SyncResult, error) {
	retryDelay := options.RetryInitialDelay
	for {
		if !time.Now().Before(leaseExpiration) {
			return SyncResult{}, markOwnershipLost(ctx, options.StateController, execution, nil)
		}
		syncCtx, cancel := context.WithDeadline(ctx, leaseExpiration)
		result, err := r.Sync(syncCtx, execution)
		cancel()
		if err == nil {
			return result, nil
		}
		if ownershipLostError(err) {
			return SyncResult{}, markOwnershipLost(ctx, options.StateController, execution, err)
		}
		wake := time.Now().Add(retryDelay)
		if wake.After(leaseExpiration) {
			wake = leaseExpiration
		}
		if err := waitUntil(ctx, wake); err != nil {
			return SyncResult{}, err
		}
		retryDelay = min(retryDelay*2, options.RetryMaximumDelay)
	}
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func ownershipLostError(err error) bool {
	var failedPrecondition *serviceerror.FailedPrecondition
	var invalidArgument *serviceerror.InvalidArgument
	var permissionDenied *serviceerror.PermissionDenied
	if errors.As(err, &failedPrecondition) ||
		errors.As(err, &invalidArgument) ||
		errors.As(err, &permissionDenied) {
		return true
	}
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

func markOwnershipLost(
	ctx context.Context,
	controller *ExecutionStateController,
	execution *commonpb.WorkflowExecution,
	cause error,
) error {
	if err := controller.Update(
		ctx,
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST,
	); err != nil {
		return fmt.Errorf("invalidate local execution after ownership loss: %w", err)
	}
	if cause != nil {
		return fmt.Errorf("%w: %v", ErrLocalExecutionOwnershipLost, cause)
	}
	return fmt.Errorf("%w: ownership lease expired", ErrLocalExecutionOwnershipLost)
}

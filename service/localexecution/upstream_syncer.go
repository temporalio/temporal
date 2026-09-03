package localexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ProtocolVersion = 1

type UpstreamSyncer struct {
	historyClient   historyservice.HistoryServiceClient
	eventSerializer serialization.Serializer
}

func NewUpstreamSyncer(
	historyClient historyservice.HistoryServiceClient,
	eventSerializer serialization.Serializer,
) *UpstreamSyncer {
	return &UpstreamSyncer{
		historyClient:   historyClient,
		eventSerializer: eventSerializer,
	}
}

func (s *UpstreamSyncer) Sync(
	ctx context.Context,
	namespaceID string,
	request *adminservice.SyncLocalExecutionRequest,
) (*adminservice.SyncLocalExecutionResponse, error) {
	if err := s.validateRequest(request); err != nil {
		return nil, err
	}
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, serviceerror.NewInternal("failed to fingerprint local execution sync request")
	}
	requestHash := sha256.Sum256(requestBytes)

	currentState, err := s.currentState(ctx, namespaceID, request.GetExecution())
	if err != nil {
		return nil, fmt.Errorf("read upstream cursor before sync: %w", err)
	}
	current := currentState.cursor
	requested := SyncCursor{EventID: request.GetNewEventId(), Version: request.GetNewEventVersion()}
	previous := SyncCursor{
		EventID: request.GetPreviousEventId(),
		Version: request.GetPreviousEventVersion(),
	}
	if localInfo := currentState.localExecutionInfo; request.GetRelease() &&
		localInfo.GetState() == persistencespb.LocalExecutionInfo_STATE_UNOWNED &&
		localInfo.GetLastSyncId() == request.GetSyncId() && current == requested {
		if !bytes.Equal(localInfo.GetLastSyncRequestHash(), requestHash[:]) {
			return nil, serviceerror.NewFailedPrecondition("sync ID was already used for a different request")
		}
		return newSyncResponse(request.GetSyncId(), requested, nil), nil
	}
	if err := validateOwnership(request, currentState.localExecutionInfo); err != nil {
		return nil, err
	}
	if current != requested && current != previous {
		return nil, serviceerror.NewFailedPreconditionf(
			"local execution cursor mismatch: upstream is at %d/%d, request starts at %d/%d",
			current.EventID,
			current.Version,
			previous.EventID,
			previous.Version,
		)
	}

	if current != requested {
		if err := s.synchronizeHistory(
			ctx,
			namespaceID,
			request,
			previous,
			currentState.localExecutionInfo,
			requestHash[:],
		); err != nil {
			return nil, err
		}
	}

	leaseExpirationTime := currentState.localExecutionInfo.GetLeaseExpirationTime()
	if currentState.localExecutionInfo != nil {
		renewal, err := s.renewLease(
			ctx,
			namespaceID,
			request,
			previous,
			requested,
			request.GetSyncId(),
			requestHash[:],
			request.GetRelease(),
		)
		if err != nil {
			return nil, fmt.Errorf("renew local execution lease: %w", err)
		}
		leaseExpirationTime = renewal.GetLeaseExpirationTime()
	}
	return newSyncResponse(request.GetSyncId(), requested, leaseExpirationTime), nil
}

func (s *UpstreamSyncer) synchronizeHistory(
	ctx context.Context,
	namespaceID string,
	request *adminservice.SyncLocalExecutionRequest,
	previous SyncCursor,
	localInfo *persistencespb.LocalExecutionInfo,
	requestHash []byte,
) error {
	if localInfo != nil {
		if _, err := s.renewLease(
			ctx,
			namespaceID,
			request,
			previous,
			previous,
			request.GetSyncId()+"/lease",
			requestHash,
			false,
		); err != nil {
			return fmt.Errorf("renew local execution lease before sync: %w", err)
		}
	}
	acknowledged := previous
	for index, batch := range request.GetHistoryBatches() {
		if err := s.replicateHistoryBatch(ctx, &historyservice.ReplicateEventsV2Request{
			NamespaceId:         namespaceID,
			WorkflowExecution:   request.GetExecution(),
			VersionHistoryItems: request.GetVersionHistory().GetItems(),
			Events:              batch,
		}); err != nil {
			return fmt.Errorf("replicate history batch %d: %w", index, err)
		}
		batchCursor, err := s.historyBatchCursor(batch)
		if err != nil {
			return fmt.Errorf("read synchronized history batch %d: %w", index, err)
		}
		if err := s.waitForCursor(ctx, namespaceID, request.GetExecution(), acknowledged, batchCursor); err != nil {
			return fmt.Errorf("wait for synchronized history batch %d: %w", index, err)
		}
		acknowledged = batchCursor
	}
	return nil
}

func (s *UpstreamSyncer) renewLease(
	ctx context.Context,
	namespaceID string,
	request *adminservice.SyncLocalExecutionRequest,
	previous SyncCursor,
	requested SyncCursor,
	syncID string,
	requestHash []byte,
	release bool,
) (*historyservice.RenewLocalExecutionLeaseResponse, error) {
	return s.historyClient.RenewLocalExecutionLease(
		ctx,
		&historyservice.RenewLocalExecutionLeaseRequest{
			NamespaceId:          namespaceID,
			Execution:            request.GetExecution(),
			LocalServerId:        request.GetLocalServerId(),
			OwnershipToken:       request.GetOwnershipToken(),
			FencingEpoch:         request.GetFencingEpoch(),
			PreviousEventId:      previous.EventID,
			PreviousEventVersion: previous.Version,
			NewEventId:           requested.EventID,
			NewEventVersion:      requested.Version,
			SyncId:               syncID,
			SyncRequestHash:      requestHash,
			Release:              release,
		},
	)
}

type upstreamState struct {
	cursor             SyncCursor
	localExecutionInfo *persistencespb.LocalExecutionInfo
}

func validateOwnership(
	request *adminservice.SyncLocalExecutionRequest,
	localInfo *persistencespb.LocalExecutionInfo,
) error {
	// This compatibility path supports histories imported before acquisition was implemented.
	if localInfo == nil {
		if len(request.GetOwnershipToken()) != 0 || request.GetFencingEpoch() != 0 {
			return serviceerror.NewFailedPrecondition("workflow execution has no local execution owner")
		}
		return nil
	}
	if localInfo.GetState() != persistencespb.LocalExecutionInfo_STATE_OWNED {
		return serviceerror.NewFailedPrecondition("workflow execution has no active local execution owner")
	}
	if localInfo.GetLocalServerId() != request.GetLocalServerId() ||
		localInfo.GetFencingEpoch() != request.GetFencingEpoch() {
		return serviceerror.NewFailedPrecondition("local execution owner is fenced")
	}
	if err := localInfo.GetLeaseExpirationTime().CheckValid(); err != nil {
		return serviceerror.NewInternal("local execution owner has an invalid lease expiration")
	}
	if !time.Now().Before(localInfo.GetLeaseExpirationTime().AsTime()) {
		return serviceerror.NewFailedPrecondition("local execution ownership lease has expired")
	}
	tokenHash := sha256.Sum256(request.GetOwnershipToken())
	if subtle.ConstantTimeCompare(tokenHash[:], localInfo.GetOwnershipTokenHash()) != 1 {
		return serviceerror.NewFailedPrecondition("local execution ownership token is invalid")
	}
	return nil
}

func (s *UpstreamSyncer) historyBatchCursor(batch *commonpb.DataBlob) (SyncCursor, error) {
	events, err := s.eventSerializer.DeserializeEvents(batch)
	if err != nil {
		return SyncCursor{}, err
	}
	if len(events) == 0 {
		return SyncCursor{}, serviceerror.NewInvalidArgument("history batches must not be empty")
	}
	lastEvent := events[len(events)-1]
	return SyncCursor{EventID: lastEvent.GetEventId(), Version: lastEvent.GetVersion()}, nil
}

func (s *UpstreamSyncer) validateRequest(request *adminservice.SyncLocalExecutionRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("request is required")
	}
	if request.GetProtocolVersion() != ProtocolVersion {
		return serviceerror.NewInvalidArgumentf(
			"unsupported local execution protocol version: %d",
			request.GetProtocolVersion(),
		)
	}
	if request.GetExecution().GetWorkflowId() == "" || request.GetExecution().GetRunId() == "" {
		return serviceerror.NewInvalidArgument("workflow ID and run ID are required")
	}
	if request.GetLocalServerId() == "" {
		return serviceerror.NewInvalidArgument("local server ID is required")
	}
	if request.GetSyncId() == "" {
		return serviceerror.NewInvalidArgument("sync ID is required")
	}
	if request.GetPreviousEventId() < common.FirstEventID {
		return serviceerror.NewInvalidArgument("previous event ID must identify an existing event")
	}
	if request.GetNewEventId() < request.GetPreviousEventId() {
		return serviceerror.NewInvalidArgument("new event ID cannot precede previous event ID")
	}
	if request.GetVersionHistory() == nil {
		return serviceerror.NewInvalidArgument("version history is required")
	}
	if err := validateVersionHistory(request); err != nil {
		return err
	}
	return s.validateHistoryBatches(request)
}

func validateVersionHistory(request *adminservice.SyncLocalExecutionRequest) error {
	previous := &historyspb.VersionHistoryItem{
		EventId: request.GetPreviousEventId(),
		Version: request.GetPreviousEventVersion(),
	}
	if !versionhistory.ContainsVersionHistoryItem(
		request.GetVersionHistory(),
		previous,
	) {
		return serviceerror.NewInvalidArgument("version history does not contain the previous cursor")
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(request.GetVersionHistory())
	if err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if lastItem.GetEventId() != request.GetNewEventId() ||
		lastItem.GetVersion() != request.GetNewEventVersion() {
		return serviceerror.NewInvalidArgument("version history does not end at the new cursor")
	}
	return nil
}

func (s *UpstreamSyncer) validateHistoryBatches(request *adminservice.SyncLocalExecutionRequest) error {
	expectedEventID := request.GetPreviousEventId() + 1
	for _, batch := range request.GetHistoryBatches() {
		events, err := s.eventSerializer.DeserializeEvents(batch)
		if err != nil {
			return serviceerror.NewInvalidArgumentf("invalid history batch: %v", err)
		}
		if len(events) == 0 {
			return serviceerror.NewInvalidArgument("history batches must not be empty")
		}
		for _, event := range events {
			if event.GetEventId() != expectedEventID {
				return serviceerror.NewInvalidArgumentf(
					"history is not contiguous: expected event %d, got %d",
					expectedEventID,
					event.GetEventId(),
				)
			}
			expectedVersion, err := versionhistory.GetVersionHistoryEventVersion(
				request.GetVersionHistory(),
				event.GetEventId(),
			)
			if err != nil || event.GetVersion() != expectedVersion {
				return serviceerror.NewInvalidArgumentf(
					"event %d version does not match version history",
					event.GetEventId(),
				)
			}
			expectedEventID++
		}
	}
	if expectedEventID != request.GetNewEventId()+1 {
		return serviceerror.NewInvalidArgumentf(
			"history ends at event %d, expected %d",
			expectedEventID-1,
			request.GetNewEventId(),
		)
	}
	return nil
}

func (s *UpstreamSyncer) currentCursor(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
) (SyncCursor, error) {
	state, err := s.currentState(ctx, namespaceID, execution)
	return state.cursor, err
}

func (s *UpstreamSyncer) currentState(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
) (upstreamState, error) {
	response, err := s.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   execution,
	})
	if err != nil {
		return upstreamState{}, err
	}
	if response.GetVersionHistories() == nil {
		return upstreamState{}, serviceerror.NewInternal("upstream mutable state has no version histories")
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
	if err != nil {
		return upstreamState{}, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return upstreamState{}, err
	}
	return upstreamState{
		cursor:             SyncCursor{EventID: lastItem.GetEventId(), Version: lastItem.GetVersion()},
		localExecutionInfo: response.GetLocalExecutionInfo(),
	}, nil
}

func (s *UpstreamSyncer) replicateHistoryBatch(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
) error {
	for {
		_, err := s.historyClient.ReplicateEventsV2(ctx, request)
		if err == nil {
			return nil
		}
		if !isRetryReplicationError(err) {
			return err
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func (s *UpstreamSyncer) waitForCursor(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	previous SyncCursor,
	requested SyncCursor,
) error {
	for {
		current, err := s.currentCursor(ctx, namespaceID, execution)
		if err != nil {
			return err
		}
		if current == requested {
			return nil
		}
		if current != previous {
			return serviceerror.NewFailedPreconditionf(
				"local execution sync reached unexpected cursor %d/%d while waiting for %d/%d to advance to %d/%d",
				current.EventID,
				current.Version,
				previous.EventID,
				previous.Version,
				requested.EventID,
				requested.Version,
			)
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func newSyncResponse(
	syncID string,
	cursor SyncCursor,
	leaseExpirationTime *timestamppb.Timestamp,
) *adminservice.SyncLocalExecutionResponse {
	return &adminservice.SyncLocalExecutionResponse{
		SyncId:                   syncID,
		AcknowledgedEventId:      cursor.EventID,
		AcknowledgedEventVersion: cursor.Version,
		LeaseExpirationTime:      leaseExpirationTime,
	}
}

func isRetryReplicationError(err error) bool {
	_, ok := asRetryReplicationError(err)
	return ok
}

func asRetryReplicationError(err error) (*serviceerrors.RetryReplication, bool) {
	var retryReplication *serviceerrors.RetryReplication
	if errors.As(err, &retryReplication) {
		return retryReplication, true
	}

	converted := serviceerrors.FromStatus(status.Convert(err))
	if errors.As(converted, &retryReplication) {
		return retryReplication, true
	}
	return nil, false
}

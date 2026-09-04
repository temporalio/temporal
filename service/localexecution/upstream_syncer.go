package localexecution

import (
	"context"
	"crypto/sha256"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"google.golang.org/protobuf/proto"
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

	response, err := s.historyClient.SyncLocalExecution(ctx, &historyservice.SyncLocalExecutionRequest{
		NamespaceId:     namespaceID,
		Request:         request,
		SyncRequestHash: requestHash[:],
	})
	if err != nil {
		return nil, fmt.Errorf("synchronize local execution history: %w", err)
	}
	if response.GetResponse() == nil {
		return nil, serviceerror.NewInternal("history returned an empty local execution synchronization response")
	}
	return response.GetResponse(), nil
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

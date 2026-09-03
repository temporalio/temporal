package localexecution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
)

const defaultMaximumPageSize = 100

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
}

type HistoryReplicator struct {
	source          HistoryEndpoint
	target          ReplicationTarget
	maximumPageSize int32
	cursor          SyncCursor
	eventSerializer serialization.Serializer
}

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

	return &HistoryReplicator{
		source:          source,
		target:          target,
		maximumPageSize: defaultMaximumPageSize,
		cursor:          cursor,
		eventSerializer: serialization.NewSerializer(),
	}, nil
}

func (r *HistoryReplicator) Sync(
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (SyncResult, error) {
	if execution.GetWorkflowId() == "" || execution.GetRunId() == "" {
		return SyncResult{}, errors.New("workflow ID and run ID are required")
	}

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
			return SyncResult{}, fmt.Errorf("read source history delta: %w", err)
		}

		historyBatches = append(historyBatches, response.GetHistoryBatches()...)
		sourceVersionHistory = response
		if len(response.NextPageToken) == 0 {
			break
		}
		sourceToken = response.NextPageToken
	}

	if sourceVersionHistory == nil || sourceVersionHistory.GetVersionHistory() == nil {
		return SyncResult{}, errors.New("source returned no version history")
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(sourceVersionHistory.GetVersionHistory())
	if err != nil {
		return SyncResult{}, fmt.Errorf("read source history cursor: %w", err)
	}
	release := r.historyClosesWorkflow(historyBatches)
	syncID := uuid.NewString()
	response, err := r.target.AdminClient.SyncLocalExecution(
		ctx,
		&adminservice.SyncLocalExecutionRequest{
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
		},
	)
	if err != nil {
		return SyncResult{}, fmt.Errorf("sync local execution: %w", err)
	}
	if response.GetSyncId() != syncID ||
		response.GetAcknowledgedEventId() != lastItem.GetEventId() ||
		response.GetAcknowledgedEventVersion() != lastItem.GetVersion() {
		return SyncResult{}, errors.New("sync response did not acknowledge the requested cursor")
	}

	r.cursor = SyncCursor{EventID: lastItem.GetEventId(), Version: lastItem.GetVersion()}
	return SyncResult{
		HistoryBatches:   len(historyBatches),
		LastEventID:      lastItem.GetEventId(),
		LastEventVersion: lastItem.GetVersion(),
		Released:         response.GetLeaseExpirationTime() == nil && release,
	}, nil
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

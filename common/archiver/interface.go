package archiver

import (
	"context"

	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"

	archivergenpb "github.com/temporalio/temporal/.gen/proto/archiver"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	// ArchiveHistoryRequest is request to Archive workflow history
	ArchiveHistoryRequest struct {
		ShardID              int
		NamespaceID          string
		Namespace            string
		WorkflowID           string
		RunID                string
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
	}

	// GetHistoryRequest is the request to Get archived history
	GetHistoryRequest struct {
		NamespaceID          string
		WorkflowID           string
		RunID                string
		CloseFailoverVersion *int64
		NextPageToken        []byte
		PageSize             int
	}

	// GetHistoryResponse is the response of Get archived history
	GetHistoryResponse struct {
		HistoryBatches []*eventpb.History
		NextPageToken  []byte
	}

	// HistoryBootstrapContainer contains components needed by all history Archiver implementations
	HistoryBootstrapContainer struct {
		HistoryV2Manager persistence.HistoryManager
		Logger           log.Logger
		MetricsClient    metrics.Client
		ClusterMetadata  cluster.Metadata
		NamespaceCache   cache.NamespaceCache
	}

	// HistoryArchiver is used to archive history and read archived history
	HistoryArchiver interface {
		Archive(context.Context, URI, *ArchiveHistoryRequest, ...ArchiveOption) error
		Get(context.Context, URI, *GetHistoryRequest) (*GetHistoryResponse, error)
		ValidateURI(URI) error
	}

	// VisibilityBootstrapContainer contains components needed by all visibility Archiver implementations
	VisibilityBootstrapContainer struct {
		Logger          log.Logger
		MetricsClient   metrics.Client
		ClusterMetadata cluster.Metadata
		NamespaceCache  cache.NamespaceCache
	}

	// QueryVisibilityRequest is the request to query archived visibility records
	QueryVisibilityRequest struct {
		NamespaceID   string
		PageSize      int
		NextPageToken []byte
		Query         string
	}

	// QueryVisibilityResponse is the response of querying archived visibility records
	QueryVisibilityResponse struct {
		Executions    []*executionpb.WorkflowExecutionInfo
		NextPageToken []byte
	}

	// VisibilityArchiver is used to archive visibility and read archived visibility
	VisibilityArchiver interface {
		Archive(context.Context, URI, *archivergenpb.ArchiveVisibilityRequest, ...ArchiveOption) error
		Query(context.Context, URI, *QueryVisibilityRequest) (*QueryVisibilityResponse, error)
		ValidateURI(URI) error
	}
)

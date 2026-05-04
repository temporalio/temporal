package workers

import (
	commonpb "go.temporal.io/api/common/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	// ListWorkersParams contains parameters for listing workers.
	ListWorkersParams struct {
		Query                string
		PageSize             int
		NextPageToken        []byte // Opaque token from a previous response to resume pagination.
		IncludeSystemWorkers bool
	}

	// ListWorkersResponse contains the result of listing workers.
	ListWorkersResponse struct {
		Workers       []*workerpb.WorkerHeartbeat
		NextPageToken []byte // Opaque token for the next page; nil if no more results.
	}

	Registry interface {
		RecordWorkerHeartbeats(nsID namespace.ID, nsName namespace.Name, principal *commonpb.Principal, workerHeartbeat []*workerpb.WorkerHeartbeat)
		ListWorkers(nsID namespace.ID, params ListWorkersParams) (ListWorkersResponse, error)
		DescribeWorker(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error)
	}
)

package workers

import (
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	// ListWorkersParams contains parameters for listing workers.
	ListWorkersParams struct {
		Query         string
		PageSize      int
		NextPageToken []byte
	}

	// ListWorkersResponse contains the result of listing workers.
	ListWorkersResponse struct {
		Workers       []*workerpb.WorkerHeartbeat
		NextPageToken []byte
	}

	Registry interface {
		RecordWorkerHeartbeats(nsID namespace.ID, nsName namespace.Name, workerHeartbeat []*workerpb.WorkerHeartbeat)
		ListWorkers(nsID namespace.ID, params ListWorkersParams) (ListWorkersResponse, error)
		DescribeWorker(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error)
	}
)

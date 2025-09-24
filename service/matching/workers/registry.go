package workers

import (
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	Registry interface {
		RecordWorkerHeartbeats(nsID namespace.ID, workerHeartbeat []*workerpb.WorkerHeartbeat)
		ListWorkers(nsID namespace.ID, queue string, nextPageToken []byte) ([]*workerpb.WorkerHeartbeat, error)
		DescribeWorker(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error)
	}
)

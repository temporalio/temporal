package workers

import (
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/log"
)

type (
	Registry interface {
		RecordWorkerHeartbeat(nsID string, workerHeartbeat *workerpb.WorkerHeartbeat)
		ListWorkers(nsID string, queue string, nextPageToken []byte) ([]*workerpb.WorkerHeartbeat, error)
	}

	registryImpl struct {
		workersStore map[string]map[string]*workerpb.WorkerHeartbeat
		logger       log.Logger
	}
)

// NewRegistry creates a new worker registry
func NewRegistry(logger log.Logger) Registry {
	return &registryImpl{
		workersStore: make(map[string]map[string]*workerpb.WorkerHeartbeat),
		logger:       logger,
	}
}

func (r *registryImpl) RecordWorkerHeartbeat(nsID string, workerHeartbeat *workerpb.WorkerHeartbeat) {
	if r.workersStore[nsID] == nil {
		r.workersStore[nsID] = make(map[string]*workerpb.WorkerHeartbeat)
	}

	r.workersStore[nsID][workerHeartbeat.WorkerInstanceKey] = workerHeartbeat
}

func (r *registryImpl) ListWorkers(nsID string, _ string, _ []byte) ([]*workerpb.WorkerHeartbeat, error) {
	workerMap, exists := r.workersStore[nsID]
	if !exists {
		return nil, serviceerror.NewNamespaceNotFound(nsID)
	}

	heartbeats := make([]*workerpb.WorkerHeartbeat, 0, len(workerMap))
	for _, heartbeat := range workerMap {
		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil
}

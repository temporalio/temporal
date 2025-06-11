package workers

import (
	"go.temporal.io/api/serviceerror"
	workersb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/log"
)

type (
	Registry interface {
		RecordWorkerHeartbeat(nsID string, workerHeartbeat *workersb.WorkerHeartbeat)
		ListWorkers(nsID string, queue string, nextPageToken []byte) ([]*workersb.WorkerHeartbeat, error)
	}

	registryImpl struct {
		workersStore map[string]map[string]*workersb.WorkerHeartbeat
		logger       log.Logger
	}
)

// NewRegistry creates a new worker registry
func NewRegistry(logger log.Logger) Registry {
	return &registryImpl{
		workersStore: make(map[string]map[string]*workersb.WorkerHeartbeat),
		logger:       logger,
	}
}

func (r *registryImpl) RecordWorkerHeartbeat(nsID string, workerHeartbeat *workersb.WorkerHeartbeat) {
	if r.workersStore[nsID] == nil {
		r.workersStore[nsID] = make(map[string]*workersb.WorkerHeartbeat)
	}

	r.workersStore[nsID][workerHeartbeat.WorkerInstanceKey] = workerHeartbeat
}

func (r *registryImpl) ListWorkers(nsID string, _ string, _ []byte) ([]*workersb.WorkerHeartbeat, error) {
	workerMap, exists := r.workersStore[nsID]
	if !exists {
		return nil, serviceerror.NewNamespaceNotFound(nsID)
	}

	heartbeats := make([]*workersb.WorkerHeartbeat, 0, len(workerMap))
	for _, heartbeat := range workerMap {
		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil
}

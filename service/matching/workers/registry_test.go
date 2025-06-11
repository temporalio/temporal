package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	workersb "go.temporal.io/api/worker/v1"
)

func TestRegistryImpl_RecordWorkerHeartbeat(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*registryImpl)
		nsID            string
		workerHeartbeat *workersb.WorkerHeartbeat
		expectedWorkers int
		expectedInStore bool
		heartbeatCheck  func(*workersb.WorkerHeartbeat)
	}{
		{
			name:  "record worker in new namespace",
			setup: func(r *registryImpl) {},
			nsID:  "namespace1",
			workerHeartbeat: &workersb.WorkerHeartbeat{
				WorkerInstanceKey: "worker1",
			},
			expectedWorkers: 1,
			expectedInStore: true,
		},
		{
			name: "record worker in existing namespace",
			setup: func(r *registryImpl) {
				r.workersStore["namespace1"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace1"]["existing-worker"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "existing-worker",
				}
			},
			nsID: "namespace1",
			workerHeartbeat: &workersb.WorkerHeartbeat{
				WorkerInstanceKey: "worker2",
			},
			expectedWorkers: 2,
			expectedInStore: true,
		},
		{
			name: "update existing worker",
			setup: func(r *registryImpl) {
				r.workersStore["namespace1"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace1"]["worker1"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker1",
					TaskQueue:         "tq1",
				}
			},
			nsID: "namespace1",
			workerHeartbeat: &workersb.WorkerHeartbeat{
				WorkerInstanceKey: "worker1", // Same key, should update
				TaskQueue:         "tq2",
			},
			expectedWorkers: 1,
			expectedInStore: true,
			heartbeatCheck: func(h *workersb.WorkerHeartbeat) {
				assert.Equal(t, "tq2", h.TaskQueue, "worker heartbeat should be updated with new task queue")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &registryImpl{
				workersStore: make(map[string]map[string]*workersb.WorkerHeartbeat),
			}
			tt.setup(r)

			r.RecordWorkerHeartbeat(tt.nsID, tt.workerHeartbeat)

			// Check if namespace exists
			nsMap, exists := r.workersStore[tt.nsID]
			assert.True(t, exists, "namespace should exist")
			assert.Len(t, nsMap, tt.expectedWorkers, "unexpected number of workers")

			// Check if specific worker exists
			worker, workerExists := nsMap[tt.workerHeartbeat.WorkerInstanceKey]
			assert.Equal(t, tt.expectedInStore, workerExists, "worker existence mismatch")
			if workerExists {
				assert.Equal(t, tt.workerHeartbeat, worker, "worker heartbeat should match")
				if tt.heartbeatCheck != nil {
					tt.heartbeatCheck(worker)
				}
			}
		})
	}
}

func TestRegistryImpl_ListWorkers(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*registryImpl)
		nsID            string
		expectedCount   int
		expectedWorkers []string // WorkerInstanceKeys
		expectError     bool
	}{
		{
			name:        "list workers from non-existent namespace",
			setup:       func(r *registryImpl) {},
			nsID:        "non-existent",
			expectError: true,
		},
		{
			name: "list workers from empty namespace",
			setup: func(r *registryImpl) {
				r.workersStore["empty-ns"] = make(map[string]*workersb.WorkerHeartbeat)
			},
			nsID:            "empty-ns",
			expectedCount:   0,
			expectedWorkers: []string{},
		},
		{
			name: "list single worker",
			setup: func(r *registryImpl) {
				r.workersStore["namespace1"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace1"]["worker1"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker1",
				}
			},
			nsID:            "namespace1",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
		{
			name: "list multiple workers",
			setup: func(r *registryImpl) {
				r.workersStore["namespace1"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace1"]["worker1"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker1",
				}
				r.workersStore["namespace1"]["worker2"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker2",
				}
				r.workersStore["namespace1"]["worker3"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker3",
				}
			},
			nsID:            "namespace1",
			expectedCount:   3,
			expectedWorkers: []string{"worker1", "worker2", "worker3"},
		},
		{
			name: "list workers from specific namespace only",
			setup: func(r *registryImpl) {
				// Setup namespace1
				r.workersStore["namespace1"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace1"]["worker1"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker1",
				}
				// Setup namespace2
				r.workersStore["namespace2"] = make(map[string]*workersb.WorkerHeartbeat)
				r.workersStore["namespace2"]["worker2"] = &workersb.WorkerHeartbeat{
					WorkerInstanceKey: "worker2",
				}
			},
			nsID:            "namespace1",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &registryImpl{
				workersStore: make(map[string]map[string]*workersb.WorkerHeartbeat),
			}
			tt.setup(r)

			result, err := r.ListWorkers(tt.nsID, "", nil)
			if tt.expectError {
				assert.Error(t, err, "expected an error for non-existent namespace")
				assert.Nil(t, result, "result should be nil when an error occurs")
				return
			} else {
				assert.NoError(t, err, "unexpected error when listing workers")
			}

			assert.Len(t, result, tt.expectedCount, "unexpected number of workers returned")

			// Check that all expected workers are present
			actualWorkers := make([]string, len(result))
			for i, worker := range result {
				actualWorkers[i] = worker.WorkerInstanceKey
			}

			if tt.expectedCount > 0 {
				assert.ElementsMatch(t, tt.expectedWorkers, actualWorkers, "worker lists don't match")
			}

			// Verify all returned workers are not nil
			for _, worker := range result {
				assert.NotNil(t, worker, "returned worker should not be nil")
			}
		})
	}
}

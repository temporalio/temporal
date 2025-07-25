package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
)

func TestRegistryImpl_RecordWorkerHeartbeat(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*registryImpl)
		nsID            namespace.ID
		workerHeartbeat *workerpb.WorkerHeartbeat
		expectedWorkers int
		expectedInStore bool
		heartbeatCheck  func(*workerpb.WorkerHeartbeat)
	}{
		{
			name:  "record worker in new namespace",
			setup: func(r *registryImpl) {},
			nsID:  "namespace1",
			workerHeartbeat: &workerpb.WorkerHeartbeat{
				WorkerInstanceKey: "worker1",
			},
			expectedWorkers: 1,
			expectedInStore: true,
		},
		{
			name: "record worker in existing namespace",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "existing-worker",
				}})
			},
			nsID: "namespace1",
			workerHeartbeat: &workerpb.WorkerHeartbeat{
				WorkerInstanceKey: "worker2",
			},
			expectedWorkers: 2,
			expectedInStore: true,
		},
		{
			name: "update existing worker",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
					TaskQueue:         "tq1",
				}})
			},
			nsID: "namespace1",
			workerHeartbeat: &workerpb.WorkerHeartbeat{
				WorkerInstanceKey: "worker1", // Same key, should update
				TaskQueue:         "tq2",
			},
			expectedWorkers: 1,
			expectedInStore: true,
			heartbeatCheck: func(h *workerpb.WorkerHeartbeat) {
				assert.Equal(t, "tq2", h.TaskQueue, "worker heartbeat should be updated with new task queue")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRegistryImpl(
				defaultBuckets, defaultEntryTTL, defaultMinEvictAge, defaultMaxEntries, defaultEvictionInterval,
			)
			tt.setup(r)

			r.RecordWorkerHeartbeats(tt.nsID, []*workerpb.WorkerHeartbeat{tt.workerHeartbeat})

			// Check if namespace exists
			nsBuket := r.getBucket(tt.nsID)
			nsMap, exists := nsBuket.namespaces[tt.nsID]
			assert.True(t, exists, "namespace should exist")
			assert.Len(t, nsMap, tt.expectedWorkers, "unexpected number of workers")

			// Check if specific worker exists
			workerEntry, workerEntryExists := nsMap[tt.workerHeartbeat.WorkerInstanceKey]
			assert.Equal(t, tt.expectedInStore, workerEntryExists, "worker existence mismatch")
			if workerEntryExists {
				assert.Equal(t, tt.workerHeartbeat, workerEntry.hb, "worker heartbeat should match")
				if tt.heartbeatCheck != nil {
					tt.heartbeatCheck(workerEntry.hb)
				}
			}
		})
	}
}

func TestRegistryImpl_ListWorkers(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*registryImpl)
		nsID            namespace.ID
		expectedCount   int
		expectedWorkers []string // WorkerInstanceKeys
		expectError     bool
	}{
		{
			name:            "list workers from non-existent namespace",
			setup:           func(r *registryImpl) {},
			nsID:            "non-existent",
			expectedCount:   0,
			expectedWorkers: []string{},
		},
		{
			name: "list workers from empty namespace",
			setup: func(r *registryImpl) {
			},
			nsID:            "empty-ns",
			expectedCount:   0,
			expectedWorkers: []string{},
		},
		{
			name: "list single worker",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
			},
			nsID:            "namespace1",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
		{
			name: "list multiple workers",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker2",
				}})
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker3",
				}})
			},
			nsID:            "namespace1",
			expectedCount:   3,
			expectedWorkers: []string{"worker1", "worker2", "worker3"},
		},
		{
			name: "list workers from specific namespace only",
			setup: func(r *registryImpl) {
				// Setup namespace1
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
				// Setup namespace2
				r.upsertHeartbeats("namespace2", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker2",
				}})
			},
			nsID:            "namespace1",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRegistryImpl(
				defaultBuckets, defaultEntryTTL, defaultMinEvictAge, defaultMaxEntries, defaultEvictionInterval,
			)
			tt.setup(r)

			result, err := r.ListWorkers(tt.nsID, "", nil)
			if tt.expectError {
				assert.Error(t, err, "expected an error for non-existent namespace")
				assert.Nil(t, result, "result should be nil when an error occurs")
				return
			}
			assert.NoError(t, err, "unexpected error when listing workers")
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

func TestRegistryImpl_DescribeWorker(t *testing.T) {
	tests := []struct {
		name              string
		setup             func(*registryImpl)
		nsID              namespace.ID
		workerInstanceKey string
		expectError       bool
	}{
		{
			name:              "list workers from non-existent namespace",
			setup:             func(r *registryImpl) {},
			nsID:              "non-existent",
			workerInstanceKey: "worker",
			expectError:       true,
		},
		{
			name: "list workers from empty namespace",
			setup: func(r *registryImpl) {
			},
			nsID:              "empty-ns",
			workerInstanceKey: "worker",
			expectError:       true,
		},
		{
			name: "list empty worker",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
			},
			nsID:              "namespace1",
			workerInstanceKey: "",
			expectError:       true,
		},
		{
			name: "list single worker, doesn't exist",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
			},
			nsID:              "namespace1",
			workerInstanceKey: "worker2",
			expectError:       true,
		},
		{
			name: "list single worker",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
			},
			nsID:              "namespace1",
			workerInstanceKey: "worker1",
		},
		{
			name: "list workers from specific namespace only",
			setup: func(r *registryImpl) {
				// Setup namespace1
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker1",
				}})
				// Setup namespace2
				r.upsertHeartbeats("namespace2", []*workerpb.WorkerHeartbeat{{
					WorkerInstanceKey: "worker2",
				}})
			},
			nsID:              "namespace2",
			workerInstanceKey: "worker2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRegistryImpl(
				defaultBuckets, defaultEntryTTL, defaultMinEvictAge, defaultMaxEntries, defaultEvictionInterval,
			)
			tt.setup(r)

			result, err := r.DescribeWorker(tt.nsID, tt.workerInstanceKey)
			if tt.expectError {
				assert.Error(t, err, "expected an error for non-existent namespace")
				assert.Nil(t, result, "result should be nil when an error occurs")
				return
			}
			assert.NoError(t, err, "unexpected error when listing workers")
			assert.NotNil(t, result, "result should not be nil when worker exists")
			assert.Equal(t, tt.workerInstanceKey, result.WorkerInstanceKey)
		})
	}
}

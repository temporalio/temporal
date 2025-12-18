package workers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

// Test helper config defaults
const (
	testDefaultEntryTTL         = 2 * time.Hour
	testDefaultMinEvictAge      = 2 * time.Minute
	testDefaultMaxEntries       = 1_000_000
	testDefaultEvictionInterval = 10 * time.Minute
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
			r := newRegistryImpl(RegistryParams{
				NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
				TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
				MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
				MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
				EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
				MetricsHandler:      metrics.NoopMetricsHandler,
				EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
			})
			tt.setup(r)

			r.RecordWorkerHeartbeats(tt.nsID, namespace.Name(tt.nsID+"_name"), []*workerpb.WorkerHeartbeat{tt.workerHeartbeat})

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
			r := newRegistryImpl(RegistryParams{
				NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
				TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
				MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
				MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
				EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
				MetricsHandler:      metrics.NoopMetricsHandler,
				EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
			})
			tt.setup(r)

			resp, err := r.ListWorkers(tt.nsID, ListWorkersParams{})
			if tt.expectError {
				require.Error(t, err, "expected an error for non-existent namespace")
				assert.Empty(t, resp.Workers, "result should be empty when an error occurs")
				return
			}
			require.NoError(t, err, "unexpected error when listing workers")
			result := resp.Workers
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

// Exercises the query matching functionality of ListWorkers.
func TestRegistryImpl_ListWorkersWithQuery(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*registryImpl)
		nsID            namespace.ID
		query           string
		expectedCount   int
		expectedWorkers []string // WorkerInstanceKeys
		expectedError   string   // Expected error message (empty if no error expected)
	}{
		{
			name: "valid query - basic filtering",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker1", TaskQueue: "queue1"},
					{WorkerInstanceKey: "worker2", TaskQueue: "queue2"},
				})
			},
			nsID:            "namespace1",
			query:           "WorkerInstanceKey = 'worker1'",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
		{
			name: "valid compound query - multiple conditions",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker1", TaskQueue: "queue1"},
					{WorkerInstanceKey: "worker2", TaskQueue: "queue2"},
				})
			},
			nsID:            "namespace1",
			query:           "WorkerInstanceKey = 'worker1' AND TaskQueue = 'queue1'",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"},
		},
		{
			name: "valid query - no matches",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker1", TaskQueue: "queue1"},
				})
			},
			nsID:            "namespace1",
			query:           "TaskQueue = 'non-existent-queue'",
			expectedCount:   0,
			expectedWorkers: []string{},
		},
		{
			name: "invalid query - malformed SQL",
			setup: func(r *registryImpl) {
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker1"},
				})
			},
			nsID:          "namespace1",
			query:         "invalid SQL syntax here",
			expectedError: "malformed query",
		},
		{
			name: "query on empty namespace",
			setup: func(r *registryImpl) {
				// No workers added
			},
			nsID:            "empty-namespace",
			query:           "WorkerInstanceKey = 'worker1'",
			expectedCount:   0,
			expectedWorkers: []string{},
		},
		{
			name: "query returns requested namespace only",
			setup: func(r *registryImpl) {
				// Add workers to namespace1
				r.upsertHeartbeats("namespace1", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker1", TaskQueue: "queue"},
				})
				// Add workers to namespace2
				r.upsertHeartbeats("namespace2", []*workerpb.WorkerHeartbeat{
					{WorkerInstanceKey: "worker2", TaskQueue: "queue"},
				})
			},
			nsID:            "namespace1",
			query:           "TaskQueue = 'queue'",
			expectedCount:   1,
			expectedWorkers: []string{"worker1"}, // Only worker1, not worker2 from namespace2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRegistryImpl(RegistryParams{
				NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
				TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
				MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
				MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
				EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
				MetricsHandler:      metrics.NoopMetricsHandler,
				EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
			})
			tt.setup(r)

			resp, err := r.ListWorkers(tt.nsID, ListWorkersParams{Query: tt.query})

			if tt.expectedError != "" {
				require.Error(t, err, "expected an error for invalid query")
				assert.Contains(t, err.Error(), tt.expectedError, "error message should contain expected text")
				assert.Empty(t, resp.Workers, "result should be empty when an error occurs")
				return
			}

			require.NoError(t, err, "unexpected error when listing workers with query")
			result := resp.Workers
			assert.Len(t, result, tt.expectedCount, "unexpected number of workers returned")

			// Check that all expected workers are present
			if tt.expectedCount > 0 {
				actualWorkers := make([]string, len(result))
				for i, worker := range result {
					actualWorkers[i] = worker.WorkerInstanceKey
				}
				assert.ElementsMatch(t, tt.expectedWorkers, actualWorkers, "worker lists don't match")
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
			r := newRegistryImpl(RegistryParams{
				NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
				TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
				MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
				MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
				EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
				MetricsHandler:      metrics.NoopMetricsHandler,
				EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
			})
			tt.setup(r)

			result, err := r.DescribeWorker(tt.nsID, tt.workerInstanceKey)
			if tt.expectError {
				require.Error(t, err, "expected an error for non-existent namespace")
				assert.Nil(t, result, "result should be nil when an error occurs")
				return
			}
			require.NoError(t, err, "unexpected error when listing workers")
			assert.NotNil(t, result, "result should not be nil when worker exists")
			assert.Equal(t, tt.workerInstanceKey, result.WorkerInstanceKey)
		})
	}
}

func TestRegistryImpl_ListWorkersPagination(t *testing.T) {
	r := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
		TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
		MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})

	// Add 5 workers in non-sorted order to verify sorting works
	r.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "worker-c"},
		{WorkerInstanceKey: "worker-a"},
		{WorkerInstanceKey: "worker-e"},
		{WorkerInstanceKey: "worker-b"},
		{WorkerInstanceKey: "worker-d"},
	})

	// Test page size of 2
	t.Run("first page", func(t *testing.T) {
		resp, err := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2})
		require.NoError(t, err)
		assert.Len(t, resp.Workers, 2)
		assert.Equal(t, "worker-a", resp.Workers[0].WorkerInstanceKey)
		assert.Equal(t, "worker-b", resp.Workers[1].WorkerInstanceKey)
		assert.NotNil(t, resp.NextPageToken, "should have next page token")
	})

	// Test second page
	t.Run("second page", func(t *testing.T) {
		// Get first page to get the token
		resp1, _ := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2})

		resp2, err := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2, NextPageToken: resp1.NextPageToken})
		require.NoError(t, err)
		assert.Len(t, resp2.Workers, 2)
		assert.Equal(t, "worker-c", resp2.Workers[0].WorkerInstanceKey)
		assert.Equal(t, "worker-d", resp2.Workers[1].WorkerInstanceKey)
		assert.NotNil(t, resp2.NextPageToken, "should have next page token")
	})

	// Test last page
	t.Run("last page", func(t *testing.T) {
		// Get first two pages
		resp1, _ := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2})
		resp2, _ := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2, NextPageToken: resp1.NextPageToken})

		resp3, err := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2, NextPageToken: resp2.NextPageToken})
		require.NoError(t, err)
		assert.Len(t, resp3.Workers, 1)
		assert.Equal(t, "worker-e", resp3.Workers[0].WorkerInstanceKey)
		assert.Nil(t, resp3.NextPageToken, "should not have next page token on last page")
	})
}

func TestRegistryImpl_ListWorkersPaginationWithDeletedCursor(t *testing.T) {
	// Test that pagination continues correctly even if the cursor item is deleted
	// between pagination requests.

	t.Run("cursor item deleted", func(t *testing.T) {
		// Simulate: page 1 returned workers a, b with cursor "b"
		// Before page 2, worker "b" is evicted
		// Page 2 should continue from "c" (first key > "b")
		workers := []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker-a"},
			// worker-b was deleted
			{WorkerInstanceKey: "worker-c"},
			{WorkerInstanceKey: "worker-d"},
		}

		// Create a token pointing to the deleted "worker-b"
		token, _ := json.Marshal(listWorkersPageToken{LastWorkerInstanceKey: "worker-b"})

		resp, err := paginateWorkers(workers, 2, token)
		require.NoError(t, err)
		assert.Len(t, resp.Workers, 2)
		// Should start from "worker-c" (first key > "worker-b")
		assert.Equal(t, "worker-c", resp.Workers[0].WorkerInstanceKey)
		assert.Equal(t, "worker-d", resp.Workers[1].WorkerInstanceKey)
	})

	t.Run("cursor at end deleted", func(t *testing.T) {
		// Simulate: cursor points to "worker-d" which was the last item
		// Before next request, "worker-d" is evicted
		// Should return empty (no more results)
		workers := []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker-a"},
			{WorkerInstanceKey: "worker-b"},
			{WorkerInstanceKey: "worker-c"},
			// worker-d was deleted
		}

		// Create a token pointing to the deleted "worker-d"
		token, _ := json.Marshal(listWorkersPageToken{LastWorkerInstanceKey: "worker-d"})

		resp, err := paginateWorkers(workers, 2, token)
		require.NoError(t, err)
		assert.Empty(t, resp.Workers, "should return empty when cursor is past all remaining workers")
		assert.Nil(t, resp.NextPageToken)
	})
}

func TestRegistryImpl_ListWorkersNoPagination(t *testing.T) {
	r := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
		TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
		MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})

	r.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "worker-a"},
		{WorkerInstanceKey: "worker-b"},
		{WorkerInstanceKey: "worker-c"},
	})

	// When pageSize is 0, return all workers without pagination
	resp, err := r.ListWorkers("ns1", ListWorkersParams{})
	require.NoError(t, err)
	assert.Len(t, resp.Workers, 3)
	assert.Nil(t, resp.NextPageToken, "should not have next page token when returning all")
}

func TestRegistryImpl_ListWorkersInvalidPageToken(t *testing.T) {
	r := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(10),
		TTL:                 dynamicconfig.GetDurationPropertyFn(testDefaultEntryTTL),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(testDefaultMinEvictAge),
		MaxItems:            dynamicconfig.GetIntPropertyFn(testDefaultMaxEntries),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(testDefaultEvictionInterval),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})

	r.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "worker-a"},
	})

	_, err := r.ListWorkers("ns1", ListWorkersParams{PageSize: 2, NextPageToken: []byte("invalid-json")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid next_page_token")
}

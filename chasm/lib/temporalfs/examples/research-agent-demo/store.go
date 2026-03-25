package main

import (
	"encoding/json"
	"fmt"
	"sync"

	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"github.com/temporalio/temporal-fs/pkg/store"
	pebblestore "github.com/temporalio/temporal-fs/pkg/store/pebble"
)

const manifestKey = "__demo_manifest__"

// ManifestEntry records the mapping from partition ID to topic for the report/browse commands.
// After workflow completion, result fields are populated for the HTML report.
type ManifestEntry struct {
	PartitionID  uint64 `json:"partition_id"`
	TopicName    string `json:"topic_name"`
	TopicSlug    string `json:"topic_slug"`
	FilesCreated int    `json:"files_created,omitempty"`
	BytesWritten int64  `json:"bytes_written,omitempty"`
	Retries      int    `json:"retries,omitempty"`
	Completed    bool   `json:"completed,omitempty"`
	Failed       bool   `json:"failed,omitempty"`
}

// DemoStore wraps a shared PebbleDB and provides per-workflow isolated stores.
type DemoStore struct {
	base *pebblestore.Store

	mu       sync.Mutex
	manifest []ManifestEntry
}

// NewDemoStore opens a PebbleDB at the given path with NoSync for throughput.
func NewDemoStore(path string) (*DemoStore, error) {
	s, err := pebblestore.NewNoSync(path)
	if err != nil {
		return nil, fmt.Errorf("open pebble store: %w", err)
	}
	return &DemoStore{base: s}, nil
}

// NewDemoStoreReadOnly opens a PebbleDB in read-only mode for report/browse.
func NewDemoStoreReadOnly(path string) (*DemoStore, error) {
	s, err := pebblestore.NewReadOnly(path)
	if err != nil {
		return nil, fmt.Errorf("open pebble store read-only: %w", err)
	}
	return &DemoStore{base: s}, nil
}

// Base returns the underlying store for direct access (e.g., manifest ops).
func (ds *DemoStore) Base() store.Store {
	return ds.base
}

// StoreForWorkflow returns a PrefixedStore isolated to the given partition ID.
// The caller must NOT call Close() on the returned store.
func (ds *DemoStore) StoreForWorkflow(partitionID uint64) store.Store {
	return store.NewPrefixedStore(ds.base, partitionID)
}

// RegisterWorkflow adds a workflow to the manifest and persists it.
func (ds *DemoStore) RegisterWorkflow(partitionID uint64, topic TopicEntry) error {
	ds.mu.Lock()
	ds.manifest = append(ds.manifest, ManifestEntry{
		PartitionID: partitionID,
		TopicName:   topic.Name,
		TopicSlug:   topic.Slug,
	})
	data, err := json.Marshal(ds.manifest)
	ds.mu.Unlock()
	if err != nil {
		return err
	}
	return ds.base.Set([]byte(manifestKey), data)
}

// UpdateWorkflowResult updates a manifest entry with the workflow's result data.
func (ds *DemoStore) UpdateWorkflowResult(topicSlug string, result WorkflowResult, failed bool) error {
	ds.mu.Lock()
	for i := range ds.manifest {
		if ds.manifest[i].TopicSlug == topicSlug {
			ds.manifest[i].FilesCreated = result.FilesCreated
			ds.manifest[i].BytesWritten = result.BytesWritten
			ds.manifest[i].Retries = result.Retries
			ds.manifest[i].Completed = !failed
			ds.manifest[i].Failed = failed
			break
		}
	}
	data, err := json.Marshal(ds.manifest)
	ds.mu.Unlock()
	if err != nil {
		return err
	}
	return ds.base.Set([]byte(manifestKey), data)
}

// LoadManifest reads the manifest from the store.
func (ds *DemoStore) LoadManifest() ([]ManifestEntry, error) {
	data, err := ds.base.Get([]byte(manifestKey))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var entries []ManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return entries, nil
}

// CreatePartition pre-creates a TemporalFS partition so the superblock exists
// before any Temporal activity tries to open it. This avoids race conditions
// under concurrent PebbleDB access where Open() may not see a recently
// committed superblock from a different goroutine.
func (ds *DemoStore) CreatePartition(partitionID uint64) error {
	s := store.NewPrefixedStore(ds.base, partitionID)
	// Try to open first — partition may already exist from a prior run.
	f, err := tfs.Open(s)
	if err != nil {
		f, err = tfs.Create(s, tfs.Options{ChunkSize: 64 * 1024})
		if err != nil {
			return fmt.Errorf("create partition %d: %w", partitionID, err)
		}
	}
	return f.Close()
}

// Close closes the underlying PebbleDB.
func (ds *DemoStore) Close() error {
	return ds.base.Close()
}

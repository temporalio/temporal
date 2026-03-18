package temporalfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/temporalio/temporal-fs/pkg/store"
	pebblestore "github.com/temporalio/temporal-fs/pkg/store/pebble"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// PebbleStoreProvider implements FSStoreProvider using PebbleDB via temporal-fs.
// One PebbleDB instance is created per history shard (lazy-created).
// Individual filesystem executions are isolated via PrefixedStore.
type PebbleStoreProvider struct {
	dataDir string
	logger  log.Logger

	mu   sync.Mutex
	dbs  map[int32]*pebblestore.Store
	seqs map[string]uint64 // maps "ns:fsid" → partition ID
	next uint64
}

// NewPebbleStoreProvider creates a new PebbleStoreProvider.
// dataDir is the root directory for TemporalFS PebbleDB instances.
func NewPebbleStoreProvider(dataDir string, logger log.Logger) *PebbleStoreProvider {
	return &PebbleStoreProvider{
		dataDir: dataDir,
		logger:  logger,
		dbs:     make(map[int32]*pebblestore.Store),
		seqs:    make(map[string]uint64),
		next:    1,
	}
}

func (p *PebbleStoreProvider) GetStore(shardID int32, namespaceID string, filesystemID string) (store.Store, error) {
	db, err := p.getOrCreateDB(shardID)
	if err != nil {
		return nil, err
	}

	partitionID := p.getPartitionID(namespaceID, filesystemID)
	return store.NewPrefixedStore(db, partitionID), nil
}

func (p *PebbleStoreProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for id, db := range p.dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
			p.logger.Error("Failed to close PebbleDB", tag.ShardID(id), tag.Error(err))
		}
	}
	p.dbs = make(map[int32]*pebblestore.Store)
	p.seqs = make(map[string]uint64)
	return firstErr
}

func (p *PebbleStoreProvider) getOrCreateDB(shardID int32) (*pebblestore.Store, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.dbs[shardID]; ok {
		return db, nil
	}

	dbPath := filepath.Join(p.dataDir, fmt.Sprintf("shard-%d", shardID))
	if err := os.MkdirAll(dbPath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB dir: %w", err)
	}

	db, err := pebblestore.New(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB at %s: %w", dbPath, err)
	}

	p.dbs[shardID] = db
	return db, nil
}

// getPartitionID returns a stable partition ID for a given namespace+filesystem pair.
// This is used by PrefixedStore for key isolation.
func (p *PebbleStoreProvider) getPartitionID(namespaceID string, filesystemID string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := namespaceID + ":" + filesystemID
	if id, ok := p.seqs[key]; ok {
		return id
	}
	id := p.next
	p.next++
	p.seqs[key] = id
	return id
}

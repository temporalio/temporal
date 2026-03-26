package temporalfs

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"

	"github.com/temporalio/temporal-zfs/pkg/store"
	pebblestore "github.com/temporalio/temporal-zfs/pkg/store/pebble"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// PebbleStoreProvider implements FSStoreProvider using PebbleDB via temporal-zfs.
// A single PebbleDB instance is used for all filesystem storage (lazy-created).
// Individual filesystem executions are isolated via PrefixedStore.
type PebbleStoreProvider struct {
	dataDir string
	logger  log.Logger

	mu sync.Mutex
	db *pebblestore.Store
}

// NewPebbleStoreProvider creates a new PebbleStoreProvider.
// dataDir is the root directory for TemporalZFS PebbleDB data.
func NewPebbleStoreProvider(dataDir string, logger log.Logger) *PebbleStoreProvider {
	return &PebbleStoreProvider{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (p *PebbleStoreProvider) GetStore(_ int32, namespaceID string, filesystemID string) (store.Store, error) {
	db, err := p.getOrCreateDB()
	if err != nil {
		return nil, err
	}

	partitionID := p.getPartitionID(namespaceID, filesystemID)
	return store.NewPrefixedStore(db, partitionID), nil
}

func (p *PebbleStoreProvider) DeleteStore(_ int32, namespaceID string, filesystemID string) error {
	db, err := p.getOrCreateDB()
	if err != nil {
		return err
	}

	partitionID := p.getPartitionID(namespaceID, filesystemID)
	// Delete all keys with this partition's 8-byte prefix by constructing
	// a range [prefix, prefixEnd) where prefixEnd is prefix+1.
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, partitionID)
	prefixEnd := make([]byte, 8)
	binary.BigEndian.PutUint64(prefixEnd, partitionID+1)
	return db.DeleteRange(prefix, prefixEnd)
}

func (p *PebbleStoreProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	if p.db != nil {
		err = p.db.Close()
		if err != nil {
			p.logger.Error("Failed to close PebbleDB", tag.Error(err))
		}
		p.db = nil
	}
	return err
}

func (p *PebbleStoreProvider) getOrCreateDB() (*pebblestore.Store, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		return p.db, nil
	}

	dbPath := filepath.Join(p.dataDir, "temporalfs")
	if err := os.MkdirAll(dbPath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create PebbleDB dir: %w", err)
	}

	db, err := pebblestore.New(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB at %s: %w", dbPath, err)
	}

	p.db = db
	return db, nil
}

// getPartitionID returns a deterministic partition ID for a given namespace+filesystem pair.
// Uses FNV-1a hash of the composite key so partition IDs are stable across restarts.
func (p *PebbleStoreProvider) getPartitionID(namespaceID string, filesystemID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(namespaceID))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(filesystemID))
	return h.Sum64()
}

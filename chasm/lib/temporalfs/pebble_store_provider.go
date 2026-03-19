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
// A single PebbleDB instance is used for all filesystem storage (lazy-created).
// Individual filesystem executions are isolated via PrefixedStore.
type PebbleStoreProvider struct {
	dataDir string
	logger  log.Logger

	mu   sync.Mutex
	db   *pebblestore.Store
	seqs map[string]uint64 // maps "ns:fsid" → partition ID
	next uint64
}

// NewPebbleStoreProvider creates a new PebbleStoreProvider.
// dataDir is the root directory for TemporalFS PebbleDB data.
func NewPebbleStoreProvider(dataDir string, logger log.Logger) *PebbleStoreProvider {
	return &PebbleStoreProvider{
		dataDir: dataDir,
		logger:  logger,
		seqs:    make(map[string]uint64),
		next:    1,
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
	p.seqs = make(map[string]uint64)
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

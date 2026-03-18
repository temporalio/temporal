package temporalfs

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"

	"go.temporal.io/server/common/log"
)

// InMemoryStoreProvider implements FSStoreProvider using in-memory maps.
// This is a placeholder for development and testing. The production OSS
// implementation will use PebbleDB once the temporal-fs module is integrated.
type InMemoryStoreProvider struct {
	logger log.Logger

	mu     sync.Mutex
	stores map[string]*inMemoryStore
}

// NewInMemoryStoreProvider creates a new InMemoryStoreProvider.
func NewInMemoryStoreProvider(logger log.Logger) *InMemoryStoreProvider {
	return &InMemoryStoreProvider{
		logger: logger,
		stores: make(map[string]*inMemoryStore),
	}
}

func (p *InMemoryStoreProvider) GetStore(_ int32, namespaceID string, filesystemID string) (FSStore, error) {
	key := string(makeExecutionPrefix(namespaceID, filesystemID))

	p.mu.Lock()
	defer p.mu.Unlock()

	if store, ok := p.stores[key]; ok {
		return store, nil
	}

	store := &inMemoryStore{
		data: make(map[string][]byte),
	}
	p.stores[key] = store
	return store, nil
}

func (p *InMemoryStoreProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stores = make(map[string]*inMemoryStore)
	return nil
}

// makeExecutionPrefix creates a unique byte prefix for a filesystem execution.
func makeExecutionPrefix(namespaceID string, filesystemID string) []byte {
	prefix := make([]byte, 0, 4+len(namespaceID)+4+len(filesystemID)+1)
	prefix = binary.BigEndian.AppendUint32(prefix, uint32(len(namespaceID)))
	prefix = append(prefix, namespaceID...)
	prefix = binary.BigEndian.AppendUint32(prefix, uint32(len(filesystemID)))
	prefix = append(prefix, filesystemID...)
	prefix = append(prefix, '/')
	return prefix
}

// inMemoryStore is a simple in-memory FSStore implementation.
type inMemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func (s *inMemoryStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[string(key)]
	if !ok {
		return nil, nil
	}
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (s *inMemoryStore) Set(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	val := make([]byte, len(value))
	copy(val, value)
	s.data[string(key)] = val
	return nil
}

func (s *inMemoryStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, string(key))
	return nil
}

func (s *inMemoryStore) NewBatch() FSBatch {
	return &inMemoryBatch{store: s}
}

func (s *inMemoryStore) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect and sort keys for deterministic iteration.
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if k >= string(start) && k < string(end) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, k := range keys {
		if !fn([]byte(k), s.data[k]) {
			break
		}
	}
	return nil
}

func (s *inMemoryStore) Close() error {
	return nil
}

type batchOp struct {
	key    string
	value  []byte
	delete bool
}

type inMemoryBatch struct {
	store *inMemoryStore
	ops   []batchOp
}

func (b *inMemoryBatch) Set(key []byte, value []byte) error {
	val := make([]byte, len(value))
	copy(val, value)
	b.ops = append(b.ops, batchOp{key: string(key), value: val})
	return nil
}

func (b *inMemoryBatch) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{key: string(key), delete: true})
	return nil
}

func (b *inMemoryBatch) Commit() error {
	if b.store == nil {
		return errors.New("batch already closed")
	}
	b.store.mu.Lock()
	defer b.store.mu.Unlock()

	for _, op := range b.ops {
		if op.delete {
			delete(b.store.data, op.key)
		} else {
			b.store.data[op.key] = op.value
		}
	}
	b.ops = nil
	return nil
}

func (b *inMemoryBatch) Close() error {
	b.ops = nil
	b.store = nil
	return nil
}

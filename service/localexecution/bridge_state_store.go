package localexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	bridgeStateVersion      = 1
	bridgeIdentityFilename  = "bridge.json"
	bridgeLockFilename      = "bridge.lock"
	bridgeDatabaseFilename  = "temporal.sqlite"
	bridgeExecutionsDirname = "executions"
)

type BridgeExecutionPhase string

const (
	BridgeExecutionPhaseImporting BridgeExecutionPhase = "IMPORTING"
	BridgeExecutionPhaseReady     BridgeExecutionPhase = "READY"
)

type BridgeExecutionRecord struct {
	Version                      int                  `json:"version"`
	Phase                        BridgeExecutionPhase `json:"phase"`
	Namespace                    string               `json:"namespace"`
	NamespaceID                  string               `json:"namespace_id"`
	WorkflowID                   string               `json:"workflow_id"`
	RunID                        string               `json:"run_id"`
	OwnershipToken               []byte               `json:"ownership_token"`
	FencingEpoch                 int64                `json:"fencing_epoch"`
	LeaseExpiration              time.Time            `json:"lease_expiration"`
	SyncIntervalMilliseconds     int64                `json:"sync_interval_milliseconds"`
	LastSynchronizedEventID      int64                `json:"last_synchronized_event_id"`
	LastSynchronizedEventVersion int64                `json:"last_synchronized_event_version"`
}

type bridgeIdentity struct {
	Version       int    `json:"version"`
	LocalServerID string `json:"local_server_id"`
}

type BridgeStateStore struct {
	mu            sync.Mutex
	directory     string
	executionsDir string
	localServerID string
	lockFile      *os.File
}

func OpenBridgeStateStore(directory string) (*BridgeStateStore, error) {
	if directory == "" {
		return nil, errors.New("bridge state directory is required")
	}
	absoluteDirectory, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve bridge state directory: %w", err)
	}
	if err := os.MkdirAll(absoluteDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create bridge state directory: %w", err)
	}
	if err := validatePrivateDirectory(absoluteDirectory); err != nil {
		return nil, err
	}

	lockFile, err := os.OpenFile(
		filepath.Join(absoluteDirectory, bridgeLockFilename),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open bridge state lock: %w", err)
	}
	if err := tryLockBridgeState(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("bridge state directory is already in use: %w", err)
	}

	store := &BridgeStateStore{
		directory:     absoluteDirectory,
		executionsDir: filepath.Join(absoluteDirectory, bridgeExecutionsDirname),
		lockFile:      lockFile,
	}
	if err := os.MkdirAll(store.executionsDir, 0o700); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create bridge execution state directory: %w", err)
	}
	if err := validatePrivateDirectory(store.executionsDir); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.loadOrCreateIdentity(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *BridgeStateStore) LocalServerID() string {
	return s.localServerID
}

func (s *BridgeStateStore) DatabasePath() string {
	return filepath.Join(s.directory, bridgeDatabaseFilename)
}

func (s *BridgeStateStore) SaveExecution(record BridgeExecutionRecord) error {
	if err := validateBridgeExecutionRecord(record); err != nil {
		return err
	}
	record.Version = bridgeStateVersion

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return errors.New("bridge state store is closed")
	}
	return writeAtomicJSON(s.executionPath(record.Namespace, record.WorkflowID, record.RunID), record)
}

func (s *BridgeStateStore) LoadExecutions() ([]BridgeExecutionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return nil, errors.New("bridge state store is closed")
	}
	entries, err := os.ReadDir(s.executionsDir)
	if err != nil {
		return nil, fmt.Errorf("read bridge execution state directory: %w", err)
	}
	records := make([]BridgeExecutionRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var record BridgeExecutionRecord
		if err := readPrivateJSON(filepath.Join(s.executionsDir, entry.Name()), &record); err != nil {
			return nil, fmt.Errorf("read bridge execution record %q: %w", entry.Name(), err)
		}
		if record.Version != bridgeStateVersion {
			return nil, fmt.Errorf("bridge execution record %q has unsupported version %d", entry.Name(), record.Version)
		}
		if err := validateBridgeExecutionRecord(record); err != nil {
			return nil, fmt.Errorf("validate bridge execution record %q: %w", entry.Name(), err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *BridgeStateStore) DeleteExecution(namespace string, workflowID string, runID string) error {
	if namespace == "" || workflowID == "" || runID == "" {
		return errors.New("namespace, workflow ID, and run ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return errors.New("bridge state store is closed")
	}
	err := os.Remove(s.executionPath(namespace, workflowID, runID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete bridge execution record: %w", err)
	}
	return syncDirectory(s.executionsDir)
}

func (s *BridgeStateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockFile == nil {
		return nil
	}
	lockFile := s.lockFile
	s.lockFile = nil
	unlockErr := unlockBridgeState(lockFile)
	closeErr := lockFile.Close()
	return errors.Join(unlockErr, closeErr)
}

func (s *BridgeStateStore) loadOrCreateIdentity() error {
	path := filepath.Join(s.directory, bridgeIdentityFilename)
	var identity bridgeIdentity
	if err := readPrivateJSON(path, &identity); err == nil {
		if identity.Version != bridgeStateVersion {
			return fmt.Errorf("bridge identity has unsupported version %d", identity.Version)
		}
		if _, err := uuid.Parse(identity.LocalServerID); err != nil {
			return errors.New("bridge identity contains an invalid local server ID")
		}
		s.localServerID = identity.LocalServerID
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read bridge identity: %w", err)
	}

	identity = bridgeIdentity{
		Version:       bridgeStateVersion,
		LocalServerID: uuid.NewString(),
	}
	if err := writeAtomicJSON(path, identity); err != nil {
		return fmt.Errorf("write bridge identity: %w", err)
	}
	s.localServerID = identity.LocalServerID
	return nil
}

func (s *BridgeStateStore) executionPath(namespace string, workflowID string, runID string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + workflowID + "\x00" + runID))
	return filepath.Join(s.executionsDir, hex.EncodeToString(digest[:])+".json")
}

func validatePrivateDirectory(directory string) error {
	info, err := os.Stat(directory)
	if err != nil {
		return fmt.Errorf("inspect bridge state directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("bridge state path must be a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("bridge state directory must not be accessible by group or other users")
	}
	return nil
}

func validateBridgeExecutionRecord(record BridgeExecutionRecord) error {
	switch record.Phase {
	case BridgeExecutionPhaseImporting, BridgeExecutionPhaseReady:
	default:
		return errors.New("execution phase is invalid")
	}
	if record.Namespace == "" || record.NamespaceID == "" {
		return errors.New("execution namespace and namespace ID are required")
	}
	if record.WorkflowID == "" || record.RunID == "" {
		return errors.New("execution workflow ID and run ID are required")
	}
	if len(record.OwnershipToken) != sha256.Size {
		return fmt.Errorf("execution ownership token must contain %d bytes", sha256.Size)
	}
	if record.FencingEpoch <= 0 {
		return errors.New("execution fencing epoch must be positive")
	}
	if record.LeaseExpiration.IsZero() {
		return errors.New("execution lease expiration is required")
	}
	if record.SyncIntervalMilliseconds <= 0 {
		return errors.New("execution sync interval must be positive")
	}
	if record.LastSynchronizedEventID <= 0 {
		return errors.New("execution synchronization cursor must be positive")
	}
	return nil
}

func readPrivateJSON(path string, target any) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("state file must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("state file must not be accessible by group or other users")
	}
	if info.Size() > 1<<20 {
		return errors.New("state file exceeds the maximum size")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("state file must contain one JSON value")
	}
	return nil
}

func writeAtomicJSON(path string, value any) (retErr error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".bridge-state-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		if retErr != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := json.NewEncoder(temporary).Encode(value); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

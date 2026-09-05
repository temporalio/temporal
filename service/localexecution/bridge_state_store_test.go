package localexecution

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBridgeStateStorePersistsIdentityAndExecutions(t *testing.T) {
	directory := privateTempDir(t)
	store, err := OpenBridgeStateStore(directory)
	require.NoError(t, err)
	serverID := store.LocalServerID()
	require.NotEmpty(t, serverID)
	require.Equal(t, filepath.Join(directory, bridgeDatabaseFilename), store.DatabasePath())

	record := validBridgeExecutionRecord()
	require.NoError(t, store.SaveExecution(record))
	recordPath := store.executionPath(record.Namespace, record.WorkflowID, record.RunID)
	info, err := os.Stat(recordPath)
	require.NoError(t, err)
	require.Zero(t, info.Mode().Perm()&0o077)
	require.NoError(t, store.Close())

	reopened, err := OpenBridgeStateStore(directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, serverID, reopened.LocalServerID())
	records, err := reopened.LoadExecutions()
	require.NoError(t, err)
	require.Len(t, records, 1)
	record.Version = bridgeStateVersion
	require.Equal(t, record, records[0])

	require.NoError(t, reopened.DeleteExecution(record.Namespace, record.WorkflowID, record.RunID))
	records, err = reopened.LoadExecutions()
	require.NoError(t, err)
	require.Empty(t, records)
	require.NoError(t, reopened.DeleteExecution(record.Namespace, record.WorkflowID, record.RunID))
}

func TestBridgeStateStorePreventsConcurrentUse(t *testing.T) {
	directory := privateTempDir(t)
	first, err := OpenBridgeStateStore(directory)
	require.NoError(t, err)

	_, err = OpenBridgeStateStore(directory)
	require.ErrorContains(t, err, "bridge state directory is already in use")
	require.NoError(t, first.Close())

	second, err := OpenBridgeStateStore(directory)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestBridgeStateStoreRejectsBroadDirectoryPermissions(t *testing.T) {
	directory := privateTempDir(t)
	require.NoError(t, os.Chmod(directory, 0o750))

	_, err := OpenBridgeStateStore(directory)
	require.EqualError(t, err, "bridge state directory must not be accessible by group or other users")
}

func TestBridgeStateStoreRejectsInvalidRecord(t *testing.T) {
	store, err := OpenBridgeStateStore(privateTempDir(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	record := validBridgeExecutionRecord()
	record.OwnershipToken = nil
	require.EqualError(t, store.SaveExecution(record), "execution ownership token must contain 32 bytes")
}

func TestBridgeStateStoreRejectsUnknownIdentityVersion(t *testing.T) {
	directory := privateTempDir(t)
	store, err := OpenBridgeStateStore(directory)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	require.NoError(t, os.WriteFile(
		filepath.Join(directory, bridgeIdentityFilename),
		[]byte(`{"version":2,"local_server_id":"5dc4aa53-1708-4885-a23b-9b82f0f068af"}`),
		0o600,
	))

	_, err = OpenBridgeStateStore(directory)
	require.EqualError(t, err, "bridge identity has unsupported version 2")
}

func TestBridgeStateStoreOperationsFailAfterClose(t *testing.T) {
	store, err := OpenBridgeStateStore(privateTempDir(t))
	require.NoError(t, err)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())

	require.EqualError(t, store.SaveExecution(validBridgeExecutionRecord()), "bridge state store is closed")
	_, err = store.LoadExecutions()
	require.EqualError(t, err, "bridge state store is closed")
}

func validBridgeExecutionRecord() BridgeExecutionRecord {
	return BridgeExecutionRecord{
		Phase:                        BridgeExecutionPhaseReady,
		Namespace:                    "namespace",
		NamespaceID:                  "namespace-id",
		WorkflowID:                   "workflow-id",
		RunID:                        "run-id",
		OwnershipToken:               bytes.Repeat([]byte{1}, 32),
		FencingEpoch:                 1,
		LeaseExpiration:              time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
		SyncIntervalMilliseconds:     3_000,
		LastSynchronizedEventID:      2,
		LastSynchronizedEventVersion: 1,
	}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0o700))
	return directory
}

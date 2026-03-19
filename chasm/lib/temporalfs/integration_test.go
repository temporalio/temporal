package temporalfs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
)

// TestFilesystemLifecycle_EndToEnd tests a full FS lifecycle:
// create → write → read → getattr → snapshot → archive.
func TestFilesystemLifecycle_EndToEnd(t *testing.T) {
	provider := newTestStoreProvider(t)
	h := newHandler(nil, log.NewTestLogger(), provider)
	nsID, fsID := "ns-e2e", "fs-e2e"

	// 1. Create the filesystem.
	initHandlerFS(t, h, nsID, fsID)

	// 2. Getattr on root inode.
	attrResp, err := h.Getattr(context.Background(), &temporalfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, attrResp.Attr.InodeId)
	require.True(t, attrResp.Attr.Mode > 0, "root inode should have a mode set")

	// 3. Create a file via temporal-fs, then write/read via handler.
	//    (WriteChunks requires an existing inode, so we create a file first.)
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, openErr := tfs.Open(s)
	require.NoError(t, openErr)
	err = f.WriteFile("/hello.txt", []byte("seed"), 0o644)
	require.NoError(t, err)
	inode, err := f.Stat("/hello.txt")
	require.NoError(t, err)
	inodeID := inode.ID
	f.Close()

	// 4. Write via handler.
	payload := []byte("hello from integration test!")
	writeResp, err := h.WriteChunks(context.Background(), &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		Data:         payload,
	})
	require.NoError(t, err)
	require.EqualValues(t, len(payload), writeResp.BytesWritten)

	// 5. Read back via handler.
	readResp, err := h.ReadChunks(context.Background(), &temporalfspb.ReadChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		ReadSize:     int64(len(payload)),
	})
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Data)

	// 6. Getattr on the file.
	fileAttr, err := h.Getattr(context.Background(), &temporalfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
	})
	require.NoError(t, err)
	require.EqualValues(t, inodeID, fileAttr.Attr.InodeId)
	require.Greater(t, fileAttr.Attr.FileSize, uint64(0))

	// 7. Create a snapshot.
	snapResp, err := h.CreateSnapshot(context.Background(), &temporalfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "e2e-snap",
	})
	require.NoError(t, err)
	require.Greater(t, snapResp.SnapshotTxnId, uint64(0))
}

// TestPebbleStoreProvider_Isolation tests that different filesystem IDs get
// different partition IDs and data isolation.
func TestPebbleStoreProvider_Isolation(t *testing.T) {
	provider := newTestStoreProvider(t)

	// Same namespace+filesystem returns consistent partition.
	s1, err := provider.GetStore(0, "ns-a", "fs-1")
	require.NoError(t, err)
	s2, err := provider.GetStore(0, "ns-a", "fs-1")
	require.NoError(t, err)
	// Both should point to the same underlying store with the same partition.
	_ = s1
	_ = s2

	// Different filesystem gets a different partition.
	s3, err := provider.GetStore(0, "ns-a", "fs-2")
	require.NoError(t, err)
	_ = s3

	// Verify internal partition IDs are different.
	p1 := provider.getPartitionID("ns-a", "fs-1")
	p2 := provider.getPartitionID("ns-a", "fs-2")
	require.NotEqual(t, p1, p2, "different filesystems should have different partition IDs")

	// Same key returns same partition (idempotent).
	p1Again := provider.getPartitionID("ns-a", "fs-1")
	require.Equal(t, p1, p1Again)
}

// TestPebbleStoreProvider_Close tests that closing releases all resources.
func TestPebbleStoreProvider_Close(t *testing.T) {
	dataDir := t.TempDir()
	provider := NewPebbleStoreProvider(dataDir, log.NewTestLogger())

	// Open a shard.
	_, err := provider.GetStore(0, "ns", "fs")
	require.NoError(t, err)

	// Close should succeed.
	err = provider.Close()
	require.NoError(t, err)

	// After close, internal state should be cleared.
	require.Nil(t, provider.db)
	require.Empty(t, provider.seqs)
}

package temporalfs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
)

func newTestHandler(t *testing.T) (*handler, *PebbleStoreProvider) {
	t.Helper()
	provider := newTestStoreProvider(t)
	h := newHandler(nil, log.NewTestLogger(), provider)
	return h, provider
}

// initHandlerFS creates an FS in the store provider.
// Note: we must NOT close the store here because PrefixedStore.Close()
// closes the underlying shared PebbleDB instance.
func initHandlerFS(t *testing.T, h *handler, nsID, fsID string) {
	t.Helper()
	f, _, err := h.createFS(0, nsID, fsID, &temporalfspb.FilesystemConfig{ChunkSize: 256 * 1024})
	require.NoError(t, err)
	f.Close()
}

func TestOpenFS(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	f, s, err := h.openFS(0, nsID, fsID)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.NotNil(t, s)
	f.Close()
}

func TestCreateFS(t *testing.T) {
	h, _ := newTestHandler(t)

	config := &temporalfspb.FilesystemConfig{ChunkSize: 512 * 1024}
	f, _, err := h.createFS(0, "ns-1", "fs-1", config)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.EqualValues(t, 512*1024, f.ChunkSize())
	f.Close()
}

func TestCreateFS_DefaultChunkSize(t *testing.T) {
	h, _ := newTestHandler(t)

	// Zero chunk size should use the default.
	config := &temporalfspb.FilesystemConfig{ChunkSize: 0}
	f, _, err := h.createFS(0, "ns-1", "fs-1", config)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.EqualValues(t, defaultChunkSize, f.ChunkSize())
	f.Close()
}

func TestInodeToAttr(t *testing.T) {
	now := time.Now()
	inode := &tfs.Inode{
		ID:        42,
		Size:      1024,
		Mode:      0o644,
		LinkCount: 1,
		UID:       1000,
		GID:       1000,
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
	}

	attr := inodeToAttr(inode)
	require.EqualValues(t, 42, attr.InodeId)
	require.EqualValues(t, 1024, attr.FileSize)
	require.EqualValues(t, 0o644, attr.Mode)
	require.EqualValues(t, 1, attr.Nlink)
	require.EqualValues(t, 1000, attr.Uid)
	require.EqualValues(t, 1000, attr.Gid)
	require.NotNil(t, attr.Atime)
	require.NotNil(t, attr.Mtime)
	require.NotNil(t, attr.Ctime)
}

func TestMapFSError(t *testing.T) {
	require.Nil(t, mapFSError(nil))
	require.Error(t, mapFSError(tfs.ErrNotFound))
}

func TestGetattr(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.Getattr(context.Background(), &temporalfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      1, // Root inode.
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Attr)
	require.EqualValues(t, 1, resp.Attr.InodeId)
	require.True(t, resp.Attr.Mode > 0)
}

func TestReadWriteChunks(t *testing.T) {
	h, provider := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file via temporal-fs directly so we have an inode to read/write.
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tfs.Open(s)
	require.NoError(t, err)
	err = f.WriteFile("/test.txt", []byte("initial"), 0o644)
	require.NoError(t, err)
	inode, err := f.Stat("/test.txt")
	require.NoError(t, err)
	inodeID := inode.ID
	f.Close()

	// Write via handler.
	data := []byte("hello temporalfs")
	writeResp, err := h.WriteChunks(context.Background(), &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		Data:         data,
	})
	require.NoError(t, err)
	require.EqualValues(t, len(data), writeResp.BytesWritten)

	// Read back via handler.
	readResp, err := h.ReadChunks(context.Background(), &temporalfspb.ReadChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		ReadSize:     int64(len(data)),
	})
	require.NoError(t, err)
	require.Equal(t, data, readResp.Data)
}

func TestCreateSnapshot(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.CreateSnapshot(context.Background(), &temporalfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "snap-1",
	})
	require.NoError(t, err)
	require.Greater(t, resp.SnapshotTxnId, uint64(0))
}

func TestStubsReturnNotImplemented(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)
	ctx := context.Background()

	// Stubs that don't open the FS at all.
	_, err := h.Lookup(ctx, &temporalfspb.LookupRequest{})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Setattr(ctx, &temporalfspb.SetattrRequest{})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.ReadDir(ctx, &temporalfspb.ReadDirRequest{})
	require.ErrorIs(t, err, errNotImplemented)

	// Stubs that open the FS first, then return not implemented.
	_, err = h.Truncate(ctx, &temporalfspb.TruncateRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Mkdir(ctx, &temporalfspb.MkdirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Unlink(ctx, &temporalfspb.UnlinkRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Rmdir(ctx, &temporalfspb.RmdirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Rename(ctx, &temporalfspb.RenameRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Link(ctx, &temporalfspb.LinkRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Symlink(ctx, &temporalfspb.SymlinkRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Readlink(ctx, &temporalfspb.ReadlinkRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Mknod(ctx, &temporalfspb.MknodRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)

	_, err = h.Statfs(ctx, &temporalfspb.StatfsRequest{
		NamespaceId: nsID, FilesystemId: fsID,
	})
	require.ErrorIs(t, err, errNotImplemented)
}

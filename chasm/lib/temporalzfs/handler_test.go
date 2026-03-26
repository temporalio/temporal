package temporalzfs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tzfs "github.com/temporalio/temporal-zfs/pkg/fs"
	temporalzfspb "go.temporal.io/server/chasm/lib/temporalzfs/gen/temporalzfspb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const rootInodeID = uint64(1)

func newTestHandler(t *testing.T) (*handler, *PebbleStoreProvider) {
	t.Helper()
	provider := newTestStoreProvider(t)
	h := newHandler(nil, log.NewTestLogger(), provider)
	return h, provider
}

// initHandlerFS creates an FS in the store provider.
func initHandlerFS(t *testing.T, h *handler, nsID, fsID string) {
	t.Helper()
	f, err := h.createFS(0, nsID, fsID, &temporalzfspb.FilesystemConfig{ChunkSize: 256 * 1024})
	require.NoError(t, err)
	_ = f.Close()
}

func TestOpenFS(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	f, err := h.openFS(0, nsID, fsID)
	require.NoError(t, err)
	require.NotNil(t, f)
	_ = f.Close()
}

func TestCreateFS(t *testing.T) {
	h, _ := newTestHandler(t)

	config := &temporalzfspb.FilesystemConfig{ChunkSize: 512 * 1024}
	f, err := h.createFS(0, "ns-1", "fs-1", config)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.EqualValues(t, 512*1024, f.ChunkSize())
	_ = f.Close()
}

func TestCreateFS_DefaultChunkSize(t *testing.T) {
	h, _ := newTestHandler(t)

	// Zero chunk size should use the default.
	config := &temporalzfspb.FilesystemConfig{ChunkSize: 0}
	f, err := h.createFS(0, "ns-1", "fs-1", config)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.EqualValues(t, defaultChunkSize, f.ChunkSize())
	_ = f.Close()
}

func TestInodeToAttr(t *testing.T) {
	now := time.Now()
	inode := &tzfs.Inode{
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
	require.NoError(t, mapFSError(nil))
	require.Error(t, mapFSError(tzfs.ErrNotFound))
}

func TestGetattr(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.Getattr(context.Background(), &temporalzfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      rootInodeID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Attr)
	require.Equal(t, rootInodeID, resp.Attr.InodeId)
	require.Positive(t, resp.Attr.Mode)
}

func TestReadWriteChunks(t *testing.T) {
	h, provider := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file via temporal-zfs directly so we have an inode to read/write.
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tzfs.Open(s)
	require.NoError(t, err)
	err = f.WriteFile("/test.txt", []byte("initial"), 0o644)
	require.NoError(t, err)
	inode, err := f.Stat("/test.txt")
	require.NoError(t, err)
	inodeID := inode.ID
	_ = f.Close()

	// Write via handler.
	data := []byte("hello temporalzfs")
	writeResp, err := h.WriteChunks(context.Background(), &temporalzfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		Data:         data,
	})
	require.NoError(t, err)
	require.EqualValues(t, len(data), writeResp.BytesWritten)

	// Read back via handler.
	readResp, err := h.ReadChunks(context.Background(), &temporalzfspb.ReadChunksRequest{
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

	resp, err := h.CreateSnapshot(context.Background(), &temporalzfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "snap-1",
	})
	require.NoError(t, err)
	require.Positive(t, resp.SnapshotTxnId)
}

func TestLookup(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a directory via handler so it shows up under root.
	mkdirResp, err := h.Mkdir(context.Background(), &temporalzfspb.MkdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "testdir",
		Mode:          0o755,
	})
	require.NoError(t, err)
	require.NotZero(t, mkdirResp.InodeId)

	// Lookup the directory by name.
	resp, err := h.Lookup(context.Background(), &temporalzfspb.LookupRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "testdir",
	})
	require.NoError(t, err)
	require.Equal(t, mkdirResp.InodeId, resp.InodeId)
	require.NotNil(t, resp.Attr)
}

func TestSetattr(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file via handler.
	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "setattr.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)
	inodeID := createResp.InodeId

	// Change mode via setattr.
	setattrResp, err := h.Setattr(context.Background(), &temporalzfspb.SetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Valid:        setattrMode,
		Attr: &temporalzfspb.InodeAttr{
			Mode: 0o600,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, setattrResp.Attr)
	require.EqualValues(t, 0o600, setattrResp.Attr.Mode)
}

func TestSetattr_Utimens(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "utimens.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)

	newTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	setattrResp, err := h.Setattr(context.Background(), &temporalzfspb.SetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      createResp.InodeId,
		Valid:        setattrMtime,
		Attr: &temporalzfspb.InodeAttr{
			Mtime: timestamppb.New(newTime),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, setattrResp.Attr)
	require.Equal(t, newTime.Unix(), setattrResp.Attr.Mtime.AsTime().Unix())
}

func TestTruncate(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file and write some data.
	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "truncate.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)
	inodeID := createResp.InodeId

	_, err = h.WriteChunks(context.Background(), &temporalzfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		Offset:       0,
		Data:         []byte("hello world"),
	})
	require.NoError(t, err)

	// Truncate to 5 bytes.
	_, err = h.Truncate(context.Background(), &temporalzfspb.TruncateRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
		NewSize:      5,
	})
	require.NoError(t, err)

	// Verify size via getattr.
	getattrResp, err := h.Getattr(context.Background(), &temporalzfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      inodeID,
	})
	require.NoError(t, err)
	require.EqualValues(t, 5, getattrResp.Attr.FileSize)
}

func TestMkdir(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.Mkdir(context.Background(), &temporalzfspb.MkdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "newdir",
		Mode:          0o755,
	})
	require.NoError(t, err)
	require.NotZero(t, resp.InodeId)
	require.NotNil(t, resp.Attr)

	// Verify via getattr.
	getattrResp, err := h.Getattr(context.Background(), &temporalzfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      resp.InodeId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.InodeId, getattrResp.Attr.InodeId)
}

func TestUnlink(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file.
	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "todelete.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)
	inodeID := createResp.InodeId

	// Unlink it.
	_, err = h.Unlink(context.Background(), &temporalzfspb.UnlinkRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "todelete.txt",
	})
	require.NoError(t, err)

	// Verify it no longer exists via lookup.
	_, err = h.Lookup(context.Background(), &temporalzfspb.LookupRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "todelete.txt",
	})
	require.Error(t, err)
	_ = inodeID
}

func TestRmdir(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a directory.
	mkdirResp, err := h.Mkdir(context.Background(), &temporalzfspb.MkdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "rmme",
		Mode:          0o755,
	})
	require.NoError(t, err)
	require.NotZero(t, mkdirResp.InodeId)

	// Rmdir it.
	_, err = h.Rmdir(context.Background(), &temporalzfspb.RmdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "rmme",
	})
	require.NoError(t, err)

	// Verify it no longer exists.
	_, err = h.Lookup(context.Background(), &temporalzfspb.LookupRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "rmme",
	})
	require.Error(t, err)
}

func TestRename(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file.
	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "original.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)

	// Rename it.
	_, err = h.Rename(context.Background(), &temporalzfspb.RenameRequest{
		NamespaceId:      nsID,
		FilesystemId:     fsID,
		OldParentInodeId: rootInodeID,
		OldName:          "original.txt",
		NewParentInodeId: rootInodeID,
		NewName:          "renamed.txt",
	})
	require.NoError(t, err)

	// Old name should not exist.
	_, err = h.Lookup(context.Background(), &temporalzfspb.LookupRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "original.txt",
	})
	require.Error(t, err)

	// New name should exist with the same inode ID.
	lookupResp, err := h.Lookup(context.Background(), &temporalzfspb.LookupRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "renamed.txt",
	})
	require.NoError(t, err)
	require.Equal(t, createResp.InodeId, lookupResp.InodeId)
}

func TestReadDir(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create two files under root.
	_, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "file-a.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)

	_, err = h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "file-b.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)

	// ReadDir on root.
	resp, err := h.ReadDir(context.Background(), &temporalzfspb.ReadDirRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      rootInodeID,
	})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 2)

	names := make(map[string]bool)
	for _, e := range resp.Entries {
		names[e.Name] = true
	}
	require.True(t, names["file-a.txt"])
	require.True(t, names["file-b.txt"])
}

func TestLink(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a file.
	createResp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "original.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)

	// Create a hard link.
	linkResp, err := h.Link(context.Background(), &temporalzfspb.LinkRequest{
		NamespaceId:      nsID,
		FilesystemId:     fsID,
		InodeId:          createResp.InodeId,
		NewParentInodeId: rootInodeID,
		NewName:          "hardlink.txt",
	})
	require.NoError(t, err)
	require.NotNil(t, linkResp.Attr)
	// Hard link should point to the same inode.
	require.Equal(t, createResp.InodeId, linkResp.Attr.InodeId)
	require.EqualValues(t, 2, linkResp.Attr.Nlink)
}

func TestSymlink(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.Symlink(context.Background(), &temporalzfspb.SymlinkRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "mylink",
		Target:        "/some/target",
	})
	require.NoError(t, err)
	require.NotZero(t, resp.InodeId)
	require.NotNil(t, resp.Attr)
}

func TestReadlink(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create symlink.
	symlinkResp, err := h.Symlink(context.Background(), &temporalzfspb.SymlinkRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "mylink",
		Target:        "/some/target",
	})
	require.NoError(t, err)

	// Readlink it back.
	readlinkResp, err := h.Readlink(context.Background(), &temporalzfspb.ReadlinkRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      symlinkResp.InodeId,
	})
	require.NoError(t, err)
	require.Equal(t, "/some/target", readlinkResp.Target)
}

func TestCreateFile(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.CreateFile(context.Background(), &temporalzfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "newfile.txt",
		Mode:          0o644,
	})
	require.NoError(t, err)
	require.NotZero(t, resp.InodeId)
	require.NotNil(t, resp.Attr)
	require.EqualValues(t, 0o644, resp.Attr.Mode)

	// Verify via getattr.
	getattrResp, err := h.Getattr(context.Background(), &temporalzfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      resp.InodeId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.InodeId, getattrResp.Attr.InodeId)
}

func TestMknod(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	// Create a FIFO (0x1000 = S_IFIFO in POSIX).
	fifoMode := uint32(0x1000 | 0o644)
	resp, err := h.Mknod(context.Background(), &temporalzfspb.MknodRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInodeID,
		Name:          "myfifo",
		Mode:          fifoMode,
		Dev:           0,
	})
	require.NoError(t, err)
	require.NotZero(t, resp.InodeId)
	require.NotNil(t, resp.Attr)
}

func TestStatfs(t *testing.T) {
	h, _ := newTestHandler(t)
	nsID, fsID := "ns-1", "fs-1"
	initHandlerFS(t, h, nsID, fsID)

	resp, err := h.Statfs(context.Background(), &temporalzfspb.StatfsRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Positive(t, resp.Blocks)
	require.Positive(t, resp.Files)
	require.Positive(t, resp.Bsize)
	require.EqualValues(t, 255, resp.Namelen)
}

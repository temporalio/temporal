package temporalfs

import (
	"context"
	"errors"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Setattr valid bitmask values (matching FUSE FATTR_* constants).
const (
	setattrMode  = 1 << 0
	setattrUID   = 1 << 1
	setattrGID   = 1 << 2
	setattrSize  = 1 << 3 // truncate
	setattrAtime = 1 << 4
	setattrMtime = 1 << 5
)

// Statfs virtual capacity defaults when no quota is configured.
const (
	statfsVirtualBytes  = 1 << 40 // 1 TiB
	statfsVirtualInodes = 1 << 20 // ~1M inodes
)

type handler struct {
	temporalfspb.UnimplementedTemporalFSServiceServer

	config        *Config
	logger        log.Logger
	storeProvider FSStoreProvider
}

func newHandler(config *Config, logger log.Logger, storeProvider FSStoreProvider) *handler {
	return &handler{
		config:        config,
		logger:        logger,
		storeProvider: storeProvider,
	}
}

// openFS obtains a store for the given filesystem and opens an fs.FS on it.
// The caller owns the returned *tfs.FS and must call f.Close() which also
// closes the underlying store. On error, all resources are cleaned up internally.
func (h *handler) openFS(shardID int32, namespaceID, filesystemID string) (*tfs.FS, error) {
	s, err := h.storeProvider.GetStore(shardID, namespaceID, filesystemID)
	if err != nil {
		return nil, mapFSError(err)
	}
	f, err := tfs.Open(s)
	if err != nil {
		_ = s.Close()
		return nil, mapFSError(err)
	}
	return f, nil
}

// createFS initializes a new filesystem in the store.
// The caller owns the returned *tfs.FS and must call f.Close() which also
// closes the underlying store. On error, all resources are cleaned up internally.
func (h *handler) createFS(shardID int32, namespaceID, filesystemID string, config *temporalfspb.FilesystemConfig) (*tfs.FS, error) {
	s, err := h.storeProvider.GetStore(shardID, namespaceID, filesystemID)
	if err != nil {
		return nil, err
	}

	chunkSize := uint32(defaultChunkSize)
	if config.GetChunkSize() > 0 {
		chunkSize = config.GetChunkSize()
	}

	f, err := tfs.Create(s, tfs.Options{ChunkSize: chunkSize})
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return f, nil
}

func (h *handler) CreateFilesystem(
	ctx context.Context,
	req *temporalfspb.CreateFilesystemRequest,
) (*temporalfspb.CreateFilesystemResponse, error) {
	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFilesystemId(),
		},
		func(mCtx chasm.MutableContext, req *temporalfspb.CreateFilesystemRequest) (*Filesystem, error) {
			fs := &Filesystem{
				FilesystemState: &temporalfspb.FilesystemState{},
				Visibility:      chasm.NewComponentField(mCtx, chasm.NewVisibilityWithData(mCtx, nil, nil)),
			}

			err := TransitionCreate.Apply(fs, mCtx, CreateEvent{
				Config:          req.GetConfig(),
				OwnerWorkflowID: req.GetOwnerWorkflowId(),
			})
			if err != nil {
				return nil, err
			}

			// Initialize the underlying FS store.
			f, createErr := h.createFS(0, req.GetNamespaceId(), req.GetFilesystemId(), fs.Config)
			if createErr != nil {
				return nil, createErr
			}
			_ = f.Close()

			return fs, nil
		},
		req,
		chasm.WithRequestID(req.GetRequestId()),
	)
	if err != nil {
		return nil, err
	}

	return &temporalfspb.CreateFilesystemResponse{
		RunId: result.ExecutionKey.RunID,
	}, nil
}

func (h *handler) GetFilesystemInfo(
	ctx context.Context,
	req *temporalfspb.GetFilesystemInfoRequest,
) (*temporalfspb.GetFilesystemInfoResponse, error) {
	ref := chasm.NewComponentRef[*Filesystem](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFilesystemId(),
	})

	return chasm.ReadComponent(
		ctx,
		ref,
		func(fs *Filesystem, ctx chasm.Context, _ *temporalfspb.GetFilesystemInfoRequest) (*temporalfspb.GetFilesystemInfoResponse, error) {
			return &temporalfspb.GetFilesystemInfoResponse{
				State: fs.FilesystemState,
				RunId: ctx.ExecutionKey().RunID,
			}, nil
		},
		req,
		nil,
	)
}

func (h *handler) ArchiveFilesystem(
	ctx context.Context,
	req *temporalfspb.ArchiveFilesystemRequest,
) (*temporalfspb.ArchiveFilesystemResponse, error) {
	ref := chasm.NewComponentRef[*Filesystem](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFilesystemId(),
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(fs *Filesystem, ctx chasm.MutableContext, _ any) (*temporalfspb.ArchiveFilesystemResponse, error) {
			if err := TransitionArchive.Apply(fs, ctx, nil); err != nil {
				return nil, err
			}
			return &temporalfspb.ArchiveFilesystemResponse{}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &temporalfspb.ArchiveFilesystemResponse{}, nil
}

// FS operations — these use temporal-fs inode-based APIs.

func (h *handler) Lookup(_ context.Context, req *temporalfspb.LookupRequest) (*temporalfspb.LookupResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.LookupByID(req.GetParentInodeId(), req.GetName())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.LookupResponse{
		InodeId: inode.ID,
		Attr:    inodeToAttr(inode),
	}, nil
}

func (h *handler) Getattr(_ context.Context, req *temporalfspb.GetattrRequest) (*temporalfspb.GetattrResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.StatByID(req.GetInodeId())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.GetattrResponse{
		Attr: inodeToAttr(inode),
	}, nil
}

func (h *handler) Setattr(_ context.Context, req *temporalfspb.SetattrRequest) (*temporalfspb.SetattrResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inodeID := req.GetInodeId()
	valid := req.GetValid()
	attr := req.GetAttr()

	if valid&setattrMode != 0 {
		if err := f.ChmodByID(inodeID, uint16(attr.GetMode())); err != nil {
			return nil, mapFSError(err)
		}
	}
	if valid&setattrUID != 0 || valid&setattrGID != 0 {
		uid := uint32(math.MaxUint32) // unchanged
		gid := uint32(math.MaxUint32)
		if valid&setattrUID != 0 {
			uid = attr.GetUid()
		}
		if valid&setattrGID != 0 {
			gid = attr.GetGid()
		}
		if err := f.ChownByID(inodeID, uid, gid); err != nil {
			return nil, mapFSError(err)
		}
	}
	if valid&setattrSize != 0 {
		if err := f.TruncateByID(inodeID, int64(attr.GetFileSize())); err != nil {
			return nil, mapFSError(err)
		}
	}
	if valid&setattrAtime != 0 || valid&setattrMtime != 0 {
		var atime, mtime time.Time
		if valid&setattrAtime != 0 && attr.GetAtime() != nil {
			atime = attr.GetAtime().AsTime()
		}
		if valid&setattrMtime != 0 && attr.GetMtime() != nil {
			mtime = attr.GetMtime().AsTime()
		}
		if err := f.UtimensByID(inodeID, atime, mtime); err != nil {
			return nil, mapFSError(err)
		}
	}

	// Re-read the inode to return updated attributes.
	inode, err := f.StatByID(inodeID)
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.SetattrResponse{
		Attr: inodeToAttr(inode),
	}, nil
}

func (h *handler) ReadChunks(_ context.Context, req *temporalfspb.ReadChunksRequest) (*temporalfspb.ReadChunksResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := f.ReadAtByID(req.GetInodeId(), req.GetOffset(), int(req.GetReadSize()))
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.ReadChunksResponse{
		Data: data,
	}, nil
}

func (h *handler) WriteChunks(_ context.Context, req *temporalfspb.WriteChunksRequest) (*temporalfspb.WriteChunksResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	err = f.WriteAtByID(req.GetInodeId(), req.GetOffset(), req.GetData())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.WriteChunksResponse{
		BytesWritten: int64(len(req.GetData())),
	}, nil
}

func (h *handler) Truncate(_ context.Context, req *temporalfspb.TruncateRequest) (*temporalfspb.TruncateResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := f.TruncateByID(req.GetInodeId(), req.GetNewSize()); err != nil {
		return nil, mapFSError(err)
	}
	return &temporalfspb.TruncateResponse{}, nil
}

func (h *handler) Mkdir(_ context.Context, req *temporalfspb.MkdirRequest) (*temporalfspb.MkdirResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.MkdirByID(req.GetParentInodeId(), req.GetName(), uint16(req.GetMode()))
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.MkdirResponse{
		InodeId: inode.ID,
		Attr:    inodeToAttr(inode),
	}, nil
}

func (h *handler) Unlink(_ context.Context, req *temporalfspb.UnlinkRequest) (*temporalfspb.UnlinkResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := f.UnlinkByID(req.GetParentInodeId(), req.GetName()); err != nil {
		return nil, mapFSError(err)
	}
	return &temporalfspb.UnlinkResponse{}, nil
}

func (h *handler) Rmdir(_ context.Context, req *temporalfspb.RmdirRequest) (*temporalfspb.RmdirResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := f.RmdirByID(req.GetParentInodeId(), req.GetName()); err != nil {
		return nil, mapFSError(err)
	}
	return &temporalfspb.RmdirResponse{}, nil
}

func (h *handler) Rename(_ context.Context, req *temporalfspb.RenameRequest) (*temporalfspb.RenameResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := f.RenameByID(
		req.GetOldParentInodeId(), req.GetOldName(),
		req.GetNewParentInodeId(), req.GetNewName(),
	); err != nil {
		return nil, mapFSError(err)
	}
	return &temporalfspb.RenameResponse{}, nil
}

func (h *handler) ReadDir(_ context.Context, req *temporalfspb.ReadDirRequest) (*temporalfspb.ReadDirResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries, err := f.ReadDirByID(req.GetInodeId())
	if err != nil {
		return nil, mapFSError(err)
	}

	protoEntries := make([]*temporalfspb.DirEntry, len(entries))
	for i, e := range entries {
		inode, err := f.StatByID(e.InodeID)
		if err != nil {
			return nil, mapFSError(err)
		}
		protoEntries[i] = &temporalfspb.DirEntry{
			Name:    e.Name,
			InodeId: e.InodeID,
			Mode:    uint32(inode.Mode),
		}
	}

	return &temporalfspb.ReadDirResponse{
		Entries: protoEntries,
	}, nil
}

func (h *handler) Link(_ context.Context, req *temporalfspb.LinkRequest) (*temporalfspb.LinkResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.LinkByID(req.GetInodeId(), req.GetNewParentInodeId(), req.GetNewName())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.LinkResponse{
		Attr: inodeToAttr(inode),
	}, nil
}

func (h *handler) Symlink(_ context.Context, req *temporalfspb.SymlinkRequest) (*temporalfspb.SymlinkResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.SymlinkByID(req.GetParentInodeId(), req.GetName(), req.GetTarget())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.SymlinkResponse{
		InodeId: inode.ID,
		Attr:    inodeToAttr(inode),
	}, nil
}

func (h *handler) Readlink(_ context.Context, req *temporalfspb.ReadlinkRequest) (*temporalfspb.ReadlinkResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	target, err := f.ReadlinkByID(req.GetInodeId())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.ReadlinkResponse{
		Target: target,
	}, nil
}

func (h *handler) CreateFile(_ context.Context, req *temporalfspb.CreateFileRequest) (*temporalfspb.CreateFileResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	inode, err := f.CreateFileByID(req.GetParentInodeId(), req.GetName(), uint16(req.GetMode()))
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.CreateFileResponse{
		InodeId: inode.ID,
		Attr:    inodeToAttr(inode),
	}, nil
}

func (h *handler) Mknod(_ context.Context, req *temporalfspb.MknodRequest) (*temporalfspb.MknodResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	typ := modeToInodeType(req.GetMode())
	inode, err := f.MknodByID(req.GetParentInodeId(), req.GetName(), uint16(req.GetMode()&0xFFF), typ, uint64(req.GetDev()))
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.MknodResponse{
		InodeId: inode.ID,
		Attr:    inodeToAttr(inode),
	}, nil
}

func (h *handler) Statfs(_ context.Context, req *temporalfspb.StatfsRequest) (*temporalfspb.StatfsResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	quota := f.GetQuota()

	bsize := uint32(f.ChunkSize())
	if bsize == 0 {
		bsize = 4096
	}

	var blocks, bfree, files, ffree uint64
	if quota.MaxBytes > 0 {
		blocks = uint64(quota.MaxBytes) / uint64(bsize)
		used := uint64(quota.UsedBytes) / uint64(bsize)
		if used > blocks {
			used = blocks
		}
		bfree = blocks - used
	} else {
		blocks = statfsVirtualBytes / uint64(bsize)
		bfree = blocks
	}
	if quota.MaxInodes > 0 {
		files = uint64(quota.MaxInodes)
		used := uint64(quota.UsedInodes)
		if used > files {
			used = files
		}
		ffree = files - used
	} else {
		files = statfsVirtualInodes
		ffree = files
	}

	return &temporalfspb.StatfsResponse{
		Blocks:  blocks,
		Bfree:   bfree,
		Bavail:  bfree,
		Files:   files,
		Ffree:   ffree,
		Bsize:   bsize,
		Namelen: 255,
		Frsize:  bsize,
	}, nil
}

func (h *handler) CreateSnapshot(_ context.Context, req *temporalfspb.CreateSnapshotRequest) (*temporalfspb.CreateSnapshotResponse, error) {
	f, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	snap, err := f.CreateSnapshot(req.GetSnapshotName())
	if err != nil {
		return nil, mapFSError(err)
	}

	return &temporalfspb.CreateSnapshotResponse{
		SnapshotTxnId: snap.TxnID,
	}, nil
}

// modeToInodeType extracts the inode type from POSIX mode bits.
func modeToInodeType(mode uint32) tfs.InodeType {
	switch mode & 0xF000 {
	case 0x1000:
		return tfs.InodeTypeFIFO
	case 0x2000:
		return tfs.InodeTypeCharDev
	case 0x6000:
		return tfs.InodeTypeBlockDev
	case 0xC000:
		return tfs.InodeTypeSocket
	default:
		return tfs.InodeTypeFile
	}
}

// inodeToAttr converts a temporal-fs Inode to the proto InodeAttr.
func inodeToAttr(inode *tfs.Inode) *temporalfspb.InodeAttr {
	return &temporalfspb.InodeAttr{
		InodeId:  inode.ID,
		FileSize: inode.Size,
		Mode:     uint32(inode.Mode),
		Nlink:    inode.LinkCount,
		Uid:      inode.UID,
		Gid:      inode.GID,
		Atime:    timestamppb.New(inode.Atime),
		Mtime:    timestamppb.New(inode.Mtime),
		Ctime:    timestamppb.New(inode.Ctime),
	}
}

// mapFSError converts temporal-fs errors to appropriate gRPC service errors.
func mapFSError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, tfs.ErrNotFound), errors.Is(err, tfs.ErrSnapshotNotFound):
		return serviceerror.NewNotFound(err.Error())
	case errors.Is(err, tfs.ErrExist):
		return serviceerror.NewAlreadyExists(err.Error())
	case errors.Is(err, tfs.ErrPermission), errors.Is(err, tfs.ErrNotPermitted):
		return serviceerror.NewPermissionDenied(err.Error(), "")
	case errors.Is(err, tfs.ErrInvalidPath), errors.Is(err, tfs.ErrInvalidRename), errors.Is(err, tfs.ErrNameTooLong):
		return serviceerror.NewInvalidArgument(err.Error())
	case errors.Is(err, tfs.ErrNoSpace), errors.Is(err, tfs.ErrTooManyLinks):
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT, err.Error())
	case errors.Is(err, tfs.ErrNotDir), errors.Is(err, tfs.ErrIsDir),
		errors.Is(err, tfs.ErrNotEmpty), errors.Is(err, tfs.ErrNotSymlink),
		errors.Is(err, tfs.ErrReadOnly):
		return serviceerror.NewFailedPrecondition(err.Error())
	default:
		return err
	}
}

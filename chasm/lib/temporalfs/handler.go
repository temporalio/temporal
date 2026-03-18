package temporalfs

import (
	"context"

	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"github.com/temporalio/temporal-fs/pkg/store"
	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
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
func (h *handler) openFS(shardID int32, namespaceID, filesystemID string) (*tfs.FS, store.Store, error) {
	s, err := h.storeProvider.GetStore(shardID, namespaceID, filesystemID)
	if err != nil {
		return nil, nil, err
	}
	f, err := tfs.Open(s)
	if err != nil {
		return nil, s, err
	}
	return f, s, nil
}

// createFS initializes a new filesystem in the store.
func (h *handler) createFS(shardID int32, namespaceID, filesystemID string, config *temporalfspb.FilesystemConfig) (*tfs.FS, store.Store, error) {
	s, err := h.storeProvider.GetStore(shardID, namespaceID, filesystemID)
	if err != nil {
		return nil, nil, err
	}

	chunkSize := uint32(defaultChunkSize)
	if config.GetChunkSize() > 0 {
		chunkSize = config.GetChunkSize()
	}

	f, err := tfs.Create(s, tfs.Options{ChunkSize: chunkSize})
	if err != nil {
		return nil, s, err
	}
	return f, s, nil
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
			_, s, createErr := h.createFS(0, req.GetNamespaceId(), req.GetFilesystemId(), fs.Config)
			if createErr != nil {
				return nil, createErr
			}
			if s != nil {
				_ = s.Close()
			}

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

// FS operations — these use temporal-fs path-based APIs.

func (h *handler) Lookup(_ context.Context, req *temporalfspb.LookupRequest) (*temporalfspb.LookupResponse, error) {
	// Lookup requires resolving parent inode ID + name to a child inode.
	// temporal-fs currently only exposes path-based ReadDir; inode-based directory
	// reading requires codec-level access. Stubbed until temporal-fs adds ReadDirByID.
	return nil, errNotImplemented
}

func (h *handler) Getattr(_ context.Context, req *temporalfspb.GetattrRequest) (*temporalfspb.GetattrResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
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

func (h *handler) Setattr(_ context.Context, _ *temporalfspb.SetattrRequest) (*temporalfspb.SetattrResponse, error) {
	// TODO: Implement setattr (chmod, chown, utimens) via temporal-fs APIs.
	return nil, errNotImplemented
}

func (h *handler) ReadChunks(_ context.Context, req *temporalfspb.ReadChunksRequest) (*temporalfspb.ReadChunksResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
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
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
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
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Truncate requires a path. For inode-based truncate, we'd need path resolution.
	// TODO: Add TruncateByID to temporal-fs or resolve inode→path.
	return nil, errNotImplemented
}

func (h *handler) Mkdir(_ context.Context, req *temporalfspb.MkdirRequest) (*temporalfspb.MkdirResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Resolve parent inode, find its path, mkdir the child.
	// For P1 with inode-based ops, we need to build the path.
	// Use the parent_inode_id + name to create via MkdirByID if available.
	return nil, errNotImplemented
}

func (h *handler) Unlink(_ context.Context, req *temporalfspb.UnlinkRequest) (*temporalfspb.UnlinkResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f // TODO: Implement using UnlinkEntry or path resolution.
	return nil, errNotImplemented
}

func (h *handler) Rmdir(_ context.Context, req *temporalfspb.RmdirRequest) (*temporalfspb.RmdirResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) Rename(_ context.Context, req *temporalfspb.RenameRequest) (*temporalfspb.RenameResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) ReadDir(_ context.Context, req *temporalfspb.ReadDirRequest) (*temporalfspb.ReadDirResponse, error) {
	// ReadDir by inode ID requires codec-level access not yet exposed by temporal-fs.
	// Stubbed until temporal-fs adds ReadDirByID.
	return nil, errNotImplemented
}

func (h *handler) Link(_ context.Context, req *temporalfspb.LinkRequest) (*temporalfspb.LinkResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) Symlink(_ context.Context, req *temporalfspb.SymlinkRequest) (*temporalfspb.SymlinkResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) Readlink(_ context.Context, req *temporalfspb.ReadlinkRequest) (*temporalfspb.ReadlinkResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) CreateFile(_ context.Context, req *temporalfspb.CreateFileRequest) (*temporalfspb.CreateFileResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) Mknod(_ context.Context, req *temporalfspb.MknodRequest) (*temporalfspb.MknodResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_ = f
	return nil, errNotImplemented
}

func (h *handler) Statfs(_ context.Context, req *temporalfspb.StatfsRequest) (*temporalfspb.StatfsResponse, error) {
	// Return synthetic statfs based on filesystem config.
	ref := chasm.NewComponentRef[*Filesystem](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFilesystemId(),
	})

	_ = ref
	return nil, errNotImplemented
}

func (h *handler) CreateSnapshot(_ context.Context, req *temporalfspb.CreateSnapshotRequest) (*temporalfspb.CreateSnapshotResponse, error) {
	f, _, err := h.openFS(0, req.GetNamespaceId(), req.GetFilesystemId())
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

// mapFSError converts temporal-fs errors to appropriate gRPC errors.
func mapFSError(err error) error {
	if err == nil {
		return nil
	}
	// TODO: Map tfs.ErrNotFound → serviceerror.NewNotFound, etc.
	return err
}

package temporalfs

import (
	"context"

	"go.temporal.io/server/chasm"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/log"
)

type handler struct {
	temporalfspb.UnimplementedTemporalFSServiceServer

	config *Config
	logger log.Logger
}

func newHandler(config *Config, logger log.Logger) *handler {
	return &handler{
		config: config,
		logger: logger,
	}
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
				Config:           req.GetConfig(),
				OwnerWorkflowID: req.GetOwnerWorkflowId(),
			})
			if err != nil {
				return nil, err
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

// Stub implementations for FS operations.
// These will be fully implemented when the temporal-fs module is integrated.

func (h *handler) Lookup(ctx context.Context, req *temporalfspb.LookupRequest) (*temporalfspb.LookupResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Getattr(ctx context.Context, req *temporalfspb.GetattrRequest) (*temporalfspb.GetattrResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Setattr(ctx context.Context, req *temporalfspb.SetattrRequest) (*temporalfspb.SetattrResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) ReadChunks(ctx context.Context, req *temporalfspb.ReadChunksRequest) (*temporalfspb.ReadChunksResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) WriteChunks(ctx context.Context, req *temporalfspb.WriteChunksRequest) (*temporalfspb.WriteChunksResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Truncate(ctx context.Context, req *temporalfspb.TruncateRequest) (*temporalfspb.TruncateResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Mkdir(ctx context.Context, req *temporalfspb.MkdirRequest) (*temporalfspb.MkdirResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Unlink(ctx context.Context, req *temporalfspb.UnlinkRequest) (*temporalfspb.UnlinkResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Rmdir(ctx context.Context, req *temporalfspb.RmdirRequest) (*temporalfspb.RmdirResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Rename(ctx context.Context, req *temporalfspb.RenameRequest) (*temporalfspb.RenameResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) ReadDir(ctx context.Context, req *temporalfspb.ReadDirRequest) (*temporalfspb.ReadDirResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Link(ctx context.Context, req *temporalfspb.LinkRequest) (*temporalfspb.LinkResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Symlink(ctx context.Context, req *temporalfspb.SymlinkRequest) (*temporalfspb.SymlinkResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Readlink(ctx context.Context, req *temporalfspb.ReadlinkRequest) (*temporalfspb.ReadlinkResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) CreateFile(ctx context.Context, req *temporalfspb.CreateFileRequest) (*temporalfspb.CreateFileResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Mknod(ctx context.Context, req *temporalfspb.MknodRequest) (*temporalfspb.MknodResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) Statfs(ctx context.Context, req *temporalfspb.StatfsRequest) (*temporalfspb.StatfsResponse, error) {
	return nil, errNotImplemented
}

func (h *handler) CreateSnapshot(ctx context.Context, req *temporalfspb.CreateSnapshotRequest) (*temporalfspb.CreateSnapshotResponse, error) {
	return nil, errNotImplemented
}

package collection

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/common/log"
)

// handler implements the history-side CollectionService gRPC server. The frontend reaches it via
// the generated layered client; routing to the owning history shard is driven by the
// routing.business_id annotation on each method (the collection_id field).
type handler struct {
	collectionpb.UnimplementedCollectionServiceServer

	logger log.Logger
}

// NewHandler exposes the CollectionService server for in-process drivers (e.g. functional tests)
// that hold a chasm engine context.
func NewHandler(logger log.Logger) collectionpb.CollectionServiceServer {
	return newHandler(logger)
}

func newHandler(logger log.Logger) *handler {
	return &handler{logger: logger}
}

func (h *handler) StartCollectionExecution(
	ctx context.Context,
	req *collectionpb.StartCollectionExecutionRequest,
) (resp *collectionpb.StartCollectionExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetCollectionId(),
		},
		func(mutableContext chasm.MutableContext, _ *collectionpb.StartCollectionExecutionRequest) (*Collection, error) {
			return NewCollection(mutableContext)
		},
		req,
		chasm.WithRequestID(req.GetRequestId()),
	)
	if err != nil {
		if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
			return nil, serviceerror.NewAlreadyExistsf("collection %q is already running", req.GetCollectionId())
		}
		return nil, err
	}

	return &collectionpb.StartCollectionExecutionResponse{
		RunId: result.ExecutionKey.RunID,
	}, nil
}

func (h *handler) DescribeCollectionExecution(
	ctx context.Context,
	req *collectionpb.DescribeCollectionExecutionRequest,
) (resp *collectionpb.DescribeCollectionExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Collection](chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetCollectionId(),
		}),
		(*Collection).Describe,
		req,
	)
	if err != nil {
		return nil, err
	}

	return &collectionpb.DescribeCollectionExecutionResponse{
		State: state,
	}, nil
}

func (h *handler) CloseCollectionExecution(
	ctx context.Context,
	req *collectionpb.CloseCollectionExecutionRequest,
) (resp *collectionpb.CloseCollectionExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	result, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Collection](chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetCollectionId(),
		}),
		(*Collection).Close,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *handler) DeleteCollectionExecution(
	ctx context.Context,
	req *collectionpb.DeleteCollectionExecutionRequest,
) (resp *collectionpb.DeleteCollectionExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	if err := chasm.DeleteExecution[*Collection](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetCollectionId(),
		},
		chasm.DeleteExecutionRequest{
			TerminateComponentRequest: chasm.TerminateComponentRequest{
				Reason:   req.GetReason(),
				Identity: req.GetIdentity(),
			},
		},
	); err != nil {
		return nil, err
	}

	return &collectionpb.DeleteCollectionExecutionResponse{}, nil
}

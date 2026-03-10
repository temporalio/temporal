package tests

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	NewPayloadStoreRequest struct {
		NamespaceID      namespace.ID
		StoreID          string
		IDReusePolicy    chasm.BusinessIDReusePolicy
		IDConflictPolicy chasm.BusinessIDConflictPolicy
	}

	NewPayloadStoreResponse struct {
		RunID string
	}

	DescribePayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	DescribePayloadStoreResponse struct {
		State *testspb.TestPayloadStore
	}

	ClosePayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	ClosePayloadStoreResponse struct{}

	CancelPayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	CancelPayloadStoreResponse struct{}

	AddPayloadRequest struct {
		NamespaceID namespace.ID
		StoreID     string
		PayloadKey  string
		Payload     *commonpb.Payload
		TTL         time.Duration
	}

	AddPayloadResponse struct {
		State *testspb.TestPayloadStore
	}

	GetPayloadRequest struct {
		NamespaceID namespace.ID
		StoreID     string
		PayloadKey  string
	}

	GetPayloadResponse struct {
		Payload *commonpb.Payload
	}

	RemovePayloadRequest struct {
		NamespaceID namespace.ID
		StoreID     string
		PayloadKey  string
	}

	RemovePayloadResponse struct {
		State *testspb.TestPayloadStore
	}
)

func NewPayloadStoreHandler(
	ctx context.Context,
	request NewPayloadStoreRequest,
) (NewPayloadStoreResponse, error) {
	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: request.NamespaceID.String(),
			BusinessID:  request.StoreID,
		},
		func(mutableContext chasm.MutableContext, _ any) (*PayloadStore, error) {
			store, err := NewPayloadStore(mutableContext)
			return store, err
		},
		nil,
		chasm.WithBusinessIDPolicy(request.IDReusePolicy, request.IDConflictPolicy),
	)
	if err != nil {
		return NewPayloadStoreResponse{}, err
	}
	return NewPayloadStoreResponse{
		RunID: result.ExecutionKey.RunID,
	}, nil
}

func DescribePayloadStoreHandler(
	ctx context.Context,
	request DescribePayloadStoreRequest,
) (DescribePayloadStoreResponse, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).Describe,
		request,
	)
	if err != nil {
		return DescribePayloadStoreResponse{}, err
	}
	return DescribePayloadStoreResponse{
		State: state,
	}, nil
}

func ClosePayloadStoreHandler(
	ctx context.Context,
	request ClosePayloadStoreRequest,
) (ClosePayloadStoreResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).Close,
		nil,
	)
	return resp, err
}

func CancelPayloadStoreHandler(
	ctx context.Context,
	request CancelPayloadStoreRequest,
) (CancelPayloadStoreResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).Cancel,
		request,
	)
	return resp, err
}

func AddPayloadHandler(
	ctx context.Context,
	request AddPayloadRequest,
) (AddPayloadResponse, error) {
	state, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).AddPayload,
		request,
	)
	if err != nil {
		return AddPayloadResponse{}, err
	}
	return AddPayloadResponse{
		State: state,
	}, nil
}

func GetPayloadHandler(
	ctx context.Context,
	request GetPayloadRequest,
) (GetPayloadResponse, error) {
	payload, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).GetPayload,
		request.PayloadKey,
	)
	if err != nil {
		return GetPayloadResponse{}, err
	}
	return GetPayloadResponse{
		Payload: payload,
	}, nil
}

func RemovePayloadHandler(
	ctx context.Context,
	request RemovePayloadRequest,
) (RemovePayloadResponse, error) {
	state, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.ExecutionKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).RemovePayload,
		request.PayloadKey,
	)
	if err != nil {
		return RemovePayloadResponse{}, err
	}
	return RemovePayloadResponse{
		State: state,
	}, nil
}

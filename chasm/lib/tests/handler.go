package tests

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
)

type (
	NewPayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	NewPayloadStoreResponse struct {
		RunID string
	}

	DescribePayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	DescribePayloadStoreResponse struct {
		State *persistencespb.TestPayloadStore
	}

	ClosePayloadStoreRequest struct {
		NamespaceID namespace.ID
		StoreID     string
	}

	ClosePayloadStoreResponse struct{}

	AddPayloadRequest struct {
		NamespaceID namespace.ID
		StoreID     string
		PayloadKey  string
		Payload     *commonpb.Payload
		TTL         time.Duration
	}

	AddPayloadResponse struct {
		State *persistencespb.TestPayloadStore
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
		State *persistencespb.TestPayloadStore
	}
)

func NewPayloadStoreHandler(
	ctx context.Context,
	request NewPayloadStoreRequest,
) (NewPayloadStoreResponse, error) {
	_, entityKey, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: request.NamespaceID.String(),
			BusinessID:  request.StoreID,
		},
		func(_ chasm.MutableContext, _ any) (*PayloadStore, any, error) {
			return NewPayloadStore(), nil, nil
		},
		nil,
	)
	if err != nil {
		return NewPayloadStoreResponse{}, err
	}
	return NewPayloadStoreResponse{
		RunID: entityKey.EntityID,
	}, nil
}

func DescribePayloadStoreHandler(
	ctx context.Context,
	request DescribePayloadStoreRequest,
) (DescribePayloadStoreResponse, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.EntityKey{
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
			chasm.EntityKey{
				NamespaceID: request.NamespaceID.String(),
				BusinessID:  request.StoreID,
			},
		),
		(*PayloadStore).Close,
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
			chasm.EntityKey{
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
			chasm.EntityKey{
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
			chasm.EntityKey{
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

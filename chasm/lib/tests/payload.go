package tests

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
)

type (
	PayloadStore struct {
		State *persistence.PayloadStore

		Payloads chasm.Map[string, *commonpb.Payload]

		chasm.UnimplementedComponent
	}
)

func NewPayloadStore() *PayloadStore {
	return &PayloadStore{
		State: &persistence.PayloadStore{
			// TODO: there's a bug in the chasm engine, if the state is zero value,
			// upon loading it will become nil.
			TotalCount: 0,
			TotalSize:  1,
		},
	}
}

func (s *PayloadStore) Describe(
	_ chasm.Context,
	_ *historyservice.DescribePayloadStoreRequest,
) (*persistence.PayloadStore, error) {
	if s.State == nil {
		panic("payload store state is nil!!!!")
	}
	return common.CloneProto(s.State), nil
}

func (s *PayloadStore) AddPayload(
	mutableContext chasm.MutableContext,
	request *historyservice.AddPayloadRequest,
) (*persistence.PayloadStore, error) {
	if _, ok := s.Payloads[request.PayloadKey]; ok {
		return nil, serviceerror.NewAlreadyExistsf("payload already exists with key: %s", request.PayloadKey)
	}

	// TODO: chasm engine should initialize the map
	if s.Payloads == nil {
		s.Payloads = make(chasm.Map[string, *commonpb.Payload])
	}
	s.Payloads[request.PayloadKey] = chasm.NewDataField(mutableContext, request.Payload)
	s.State.TotalCount++
	s.State.TotalSize += int64(len(request.Payload.Data))
	return s.Describe(mutableContext, nil)
}

func (s *PayloadStore) GetPayload(
	chasmContext chasm.Context,
	key string,
) (*commonpb.Payload, error) {
	if field, ok := s.Payloads[key]; ok {
		return field.Get(chasmContext)
	}
	return nil, serviceerror.NewNotFoundf("payload not found with key: %s", key)
}

func (s *PayloadStore) RemovePayload(
	mutableContext chasm.MutableContext,
	key string,
) (*persistence.PayloadStore, error) {
	if _, ok := s.Payloads[key]; !ok {
		return nil, serviceerror.NewNotFoundf("payload not found with key: %s", key)
	}

	field := s.Payloads[key]
	payload, err := field.Get(mutableContext)
	if err != nil {
		return nil, err
	}
	s.State.TotalCount--
	s.State.TotalSize -= int64(len(payload.Data))
	delete(s.Payloads, key)
	return s.Describe(mutableContext, nil)
}

func (s *PayloadStore) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

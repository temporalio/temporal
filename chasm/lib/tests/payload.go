package tests

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TotalCountMemoFieldName = "TotalCount"
	TotalSizeMemoFieldName  = "TotalSize"
)

const (
	TestScheduleID                = "TestScheduleID"
	PayloadTotalCountSAAlias      = "PayloadTotalCount"
	PayloadTotalSizeSAAlias       = "PayloadTotalSize"
	PayloadExecutionStatusSAAlias = "PayloadExecutionStatus"
)

var (
	PayloadTotalCountSearchAttribute      = chasm.NewSearchAttributeInt(PayloadTotalCountSAAlias, chasm.SearchAttributeFieldInt01)
	PayloadTotalSizeSearchAttribute       = chasm.NewSearchAttributeInt(PayloadTotalSizeSAAlias, chasm.SearchAttributeFieldInt02)
	PayloadExecutionStatusSearchAttribute = chasm.NewSearchAttributeKeyword(PayloadExecutionStatusSAAlias, chasm.SearchAttributeFieldLowCardinalityKeyword01)

	_ chasm.VisibilitySearchAttributesProvider = (*PayloadStore)(nil)
	_ chasm.VisibilityMemoProvider             = (*PayloadStore)(nil)
)

type (
	PayloadStore struct {
		chasm.UnimplementedComponent

		State *testspb.TestPayloadStore

		Payloads   chasm.Map[string, *commonpb.Payload]
		Visibility chasm.Field[*chasm.Visibility]
	}
)

func NewPayloadStore(
	mutableContext chasm.MutableContext,
) (*PayloadStore, error) {
	store := &PayloadStore{
		State: &testspb.TestPayloadStore{
			TotalCount:      0,
			TotalSize:       0,
			ExpirationTimes: make(map[string]*timestamppb.Timestamp),
		},
		Visibility: chasm.NewComponentField(
			mutableContext,
			chasm.NewVisibility(mutableContext),
		),
	}
	return store, nil
}

func (s *PayloadStore) Describe(
	_ chasm.Context,
	_ DescribePayloadStoreRequest,
) (*testspb.TestPayloadStore, error) {
	return common.CloneProto(s.State), nil
}

func (s *PayloadStore) Close(
	_ chasm.MutableContext,
	_ ClosePayloadStoreRequest,
) (ClosePayloadStoreResponse, error) {
	s.State.Closed = true
	return ClosePayloadStoreResponse{}, nil
}

func (s *PayloadStore) AddPayload(
	mutableContext chasm.MutableContext,
	request AddPayloadRequest,
) (*testspb.TestPayloadStore, error) {
	if _, ok := s.Payloads[request.PayloadKey]; ok {
		return nil, serviceerror.NewAlreadyExistsf("payload already exists with key: %s", request.PayloadKey)
	}

	if s.Payloads == nil {
		s.Payloads = make(chasm.Map[string, *commonpb.Payload])
	}
	s.Payloads[request.PayloadKey] = chasm.NewDataField(mutableContext, request.Payload)
	s.State.TotalCount++
	s.State.TotalSize += int64(len(request.Payload.Data))

	if request.TTL > 0 {
		expirationTime := mutableContext.Now(s).Add(request.TTL)
		if s.State.ExpirationTimes == nil {
			s.State.ExpirationTimes = make(map[string]*timestamppb.Timestamp)
		}
		s.State.ExpirationTimes[request.PayloadKey] = timestamppb.New(expirationTime)
		mutableContext.AddTask(
			s,
			chasm.TaskAttributes{ScheduledTime: expirationTime},
			// You can switch between TestPayloadTTLPureTask & TestPayloadTTLSideEffectTask
			&testspb.TestPayloadTTLPureTask{
				PayloadKey: request.PayloadKey,
			},
		)
	}

	return s.Describe(mutableContext, DescribePayloadStoreRequest{})
}

func (s *PayloadStore) GetPayload(
	chasmContext chasm.Context,
	key string,
) (*commonpb.Payload, error) {
	if field, ok := s.Payloads[key]; ok {
		return field.Get(chasmContext), nil
	}
	return nil, serviceerror.NewNotFoundf("payload not found with key: %s", key)
}

func (s *PayloadStore) RemovePayload(
	mutableContext chasm.MutableContext,
	key string,
) (*testspb.TestPayloadStore, error) {
	if _, ok := s.Payloads[key]; !ok {
		return nil, serviceerror.NewNotFoundf("payload not found with key: %s", key)
	}

	field := s.Payloads[key]
	payload := field.Get(mutableContext)
	s.State.TotalCount--
	s.State.TotalSize -= int64(len(payload.Data))
	delete(s.Payloads, key)
	delete(s.State.ExpirationTimes, key)

	return s.Describe(mutableContext, DescribePayloadStoreRequest{})
}

func (s *PayloadStore) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	if s.State.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface
func (s *PayloadStore) SearchAttributes(
	ctx chasm.Context,
) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		PayloadTotalCountSearchAttribute.Value(s.State.TotalCount),
		PayloadTotalSizeSearchAttribute.Value(s.State.TotalSize),
		PayloadExecutionStatusSearchAttribute.Value(s.LifecycleState(ctx).String()),
		chasm.SearchAttributeTemporalScheduledByID.Value(TestScheduleID),
	}
}

// Memo implements chasm.VisibilityMemoProvider interface
func (s *PayloadStore) Memo(_ chasm.Context) proto.Message {
	return s.State
}

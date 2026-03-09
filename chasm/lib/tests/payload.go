package tests

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TotalCountMemoFieldName = "TotalCount"
	TotalSizeMemoFieldName  = "TotalSize"
)

const (
	TestScheduleID               = "TestScheduleID"
	PayloadTotalCountSAAlias     = "PayloadTotalCount"
	PayloadTotalSizeSAAlias      = "PayloadTotalSize"
	ExecutionStatusSAAlias       = "ExecutionStatus"
	DefaultPayloadStoreTaskQueue = "payload-store-task-queue"
)

var (
	PayloadTotalCountSearchAttribute = chasm.NewSearchAttributeInt(PayloadTotalCountSAAlias, chasm.SearchAttributeFieldInt01)
	PayloadTotalSizeSearchAttribute  = chasm.NewSearchAttributeInt(PayloadTotalSizeSAAlias, chasm.SearchAttributeFieldInt02)
	ExecutionStatusSearchAttribute   = chasm.NewSearchAttributeKeyword(ExecutionStatusSAAlias, chasm.SearchAttributeFieldLowCardinalityKeyword01)

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

	componentContextKey string
)

const (
	componentCtxKey componentContextKey = "key"
	componentCtxVal string              = "value"
)

func NewPayloadStore(
	mutableContext chasm.MutableContext,
) (*PayloadStore, error) {
	if err := assertContextValue(mutableContext); err != nil {
		return nil, err
	}

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

func assertContextValue(chasmContext chasm.Context) error {
	if val := chasmContext.Value(componentCtxKey); val != componentCtxVal {
		return softassert.UnexpectedInternalErr(
			chasmContext.Logger(),
			"registered component key value pair not available in context",
			nil,
		)
	}

	return nil
}

func (s *PayloadStore) Describe(
	chasmContext chasm.Context,
	_ DescribePayloadStoreRequest,
) (*testspb.TestPayloadStore, error) {
	if err := assertContextValue(chasmContext); err != nil {
		return nil, err
	}

	return common.CloneProto(s.State), nil
}

func (s *PayloadStore) Close(
	chasmContext chasm.MutableContext,
	_ chasm.NoValue,
) (ClosePayloadStoreResponse, error) {
	if err := assertContextValue(chasmContext); err != nil {
		return ClosePayloadStoreResponse{}, err
	}

	s.State.Closed = true
	return ClosePayloadStoreResponse{}, nil
}

func (s *PayloadStore) Cancel(
	_ chasm.MutableContext,
	_ CancelPayloadStoreRequest,
) (CancelPayloadStoreResponse, error) {
	s.State.Canceled = true
	return CancelPayloadStoreResponse{}, nil
}

func (s *PayloadStore) AddPayload(
	mutableContext chasm.MutableContext,
	request AddPayloadRequest,
) (*testspb.TestPayloadStore, error) {
	if err := assertContextValue(mutableContext); err != nil {
		return nil, err
	}

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
	if err := assertContextValue(chasmContext); err != nil {
		return nil, err
	}

	if field, ok := s.Payloads[key]; ok {
		return field.Get(chasmContext), nil
	}
	return nil, serviceerror.NewNotFoundf("payload not found with key: %s", key)
}

func (s *PayloadStore) RemovePayload(
	mutableContext chasm.MutableContext,
	key string,
) (*testspb.TestPayloadStore, error) {
	if err := assertContextValue(mutableContext); err != nil {
		return nil, err
	}

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
	chasmContext chasm.Context,
) chasm.LifecycleState {
	if err := assertContextValue(chasmContext); err != nil {
		// nolint:forbidigo // Panic here for testing.
		panic("registered component key value pair not available in context")
	}

	if s.State.Canceled {
		return chasm.LifecycleStateFailed
	}
	if s.State.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (s *PayloadStore) Terminate(
	mutableContext chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if err := assertContextValue(mutableContext); err != nil {
		return chasm.TerminateComponentResponse{}, err
	}

	if _, err := s.Close(mutableContext, nil); err != nil {
		return chasm.TerminateComponentResponse{}, err
	}
	return chasm.TerminateComponentResponse{}, nil
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface
func (s *PayloadStore) SearchAttributes(
	chasmContext chasm.Context,
) []chasm.SearchAttributeKeyValue {
	if err := assertContextValue(chasmContext); err != nil {
		// nolint:forbidigo // Panic here for testing.
		panic("registered component key value pair not available in context")
	}

	status := s.LifecycleState(chasmContext).String()
	if s.State.Canceled {
		status = "Canceled"
	}

	return []chasm.SearchAttributeKeyValue{
		PayloadTotalCountSearchAttribute.Value(s.State.TotalCount),
		PayloadTotalSizeSearchAttribute.Value(s.State.TotalSize),
		ExecutionStatusSearchAttribute.Value(status),
		chasm.SearchAttributeTemporalScheduledByID.Value(TestScheduleID),
		chasm.SearchAttributeTaskQueue.Value(DefaultPayloadStoreTaskQueue),
	}
}

// Memo implements chasm.VisibilityMemoProvider interface
func (s *PayloadStore) Memo(chasmContext chasm.Context) proto.Message {
	if err := assertContextValue(chasmContext); err != nil {
		// nolint:forbidigo // Panic here for testing.
		panic("registered component key value pair not available in context")
	}

	return s.State
}

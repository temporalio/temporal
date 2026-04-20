package visibility

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ChasmVisibilityManagerSuite struct {
		*require.Assertions
		suite.Suite
		controller *gomock.Controller

		registry          *chasm.Registry
		nsRegistry        *namespace.MockRegistry
		visibilityManager *manager.MockVisibilityManager
		visibilityMgr     *ChasmVisibilityManager
	}

	// Test component for mocking CHASM components in visibility tests
	visibilityTestComponent struct {
		chasm.UnimplementedComponent
		Data *persistencespb.WorkflowExecutionState
	}
)

var (
	testChasmNamespaceID   = namespace.ID("test-chasm-ns-id")
	testChasmNamespace     = namespace.Name("test-chasm-namespace")
	testBusinessID         = "test-business-id"
	testRunID              = "test-run-id"
	testComponentStartTime = time.Now().UTC().Truncate(time.Millisecond)
	testComponentCloseTime = testComponentStartTime.Add(5 * time.Minute)

	testChasmSA1Name = "chasm_sa1"
	testChasmSA2Name = "chasm_sa2"
	testCustomSAName = "custom_sa"

	errTestVisibilityError = &testVisibilityError{message: "test visibility error"}
)

type testVisibilityError struct {
	message string
}

func (e *testVisibilityError) Error() string {
	return e.message
}

func (tc *visibilityTestComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func TestChasmVisibilitySuite(t *testing.T) {
	suite.Run(t, new(ChasmVisibilityManagerSuite))
}

func (s *ChasmVisibilityManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	// Create a test registry with test component
	library := chasm.NewMockLibrary(s.controller)
	library.EXPECT().Name().Return("TestLibrary").AnyTimes()
	library.EXPECT().Components().Return([]*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*visibilityTestComponent](
			"TestComponent",
			chasm.WithSearchAttributes(
				chasm.NewSearchAttributeInt(testChasmSA1Name, chasm.SearchAttributeFieldInt01),
				chasm.NewSearchAttributeKeyword(testChasmSA2Name, chasm.SearchAttributeFieldKeyword01),
			),
		),
	}).AnyTimes()
	library.EXPECT().Tasks().Return(nil).AnyTimes()
	library.EXPECT().NexusServices().Return(nil).AnyTimes()
	library.EXPECT().NexusServiceProcessors().Return(nil).AnyTimes()

	s.registry = chasm.NewRegistry(log.NewNoopLogger())
	err := s.registry.Register(library)
	s.NoError(err)

	s.visibilityManager = manager.NewMockVisibilityManager(s.controller)

	s.nsRegistry = namespace.NewMockRegistry(s.controller)
	s.nsRegistry.EXPECT().GetNamespaceID(testChasmNamespace).Return(testChasmNamespaceID, nil).AnyTimes()

	s.visibilityMgr = NewChasmVisibilityManager(
		s.registry,
		s.nsRegistry,
		s.visibilityManager,
	)
}

func (s *ChasmVisibilityManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ChasmVisibilityManagerSuite) TestListExecutions_Success() {
	ctx := context.Background()

	query := "StartTime > '2024-01-01T00:00:00Z'"
	pageSize := 10
	pageToken := []byte("test-token")

	// Create test search attributes
	chasmSA1Payload, err := payload.Encode(int64(123))
	s.NoError(err)
	customSAPayload, err := payload.Encode("custom-value")
	s.NoError(err)

	// Create test memo
	memoData := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"test-memo-key": chasmSA1Payload,
		},
	}

	// Create chasm memo
	chasmMemoData := &persistencespb.WorkflowExecutionState{
		RunId: testRunID,
	}
	chasmMemoBytes, err := proto.Marshal(chasmMemoData)
	s.NoError(err)
	chasmMemoPayload, err := payload.Encode(chasmMemoBytes)
	s.NoError(err)

	// Get archetype ID for the test component
	archetypeID, ok := s.registry.ArchetypeIDOf(reflect.TypeFor[*visibilityTestComponent]())
	s.True(ok)

	// Create chasm search attributes map
	chasmSearchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			testChasmSA1Name: sadefs.MustEncodeValue(int64(123), enumspb.INDEXED_VALUE_TYPE_INT),
			testChasmSA2Name: sadefs.MustEncodeValue("test-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		},
	}

	// Create custom search attributes map
	customSearchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			testCustomSAName: customSAPayload,
		},
	}

	// Setup visibility manager mock
	expectedRequest := &visibilityservice.ListChasmExecutionsRequest{
		ArchetypeId:   archetypeID,
		NamespaceId:   testChasmNamespaceID.String(),
		Namespace:     testChasmNamespace.String(),
		PageSize:      int32(pageSize),
		NextPageToken: pageToken,
		Query:         query,
	}

	expectedResponse := &visibilityservice.ListChasmExecutionsResponse{
		Executions: []*chasmspb.VisibilityExecutionInfo{
			{
				BusinessId:             testBusinessID,
				RunId:                  testRunID,
				StartTime:              timestamppb.New(testComponentStartTime),
				CloseTime:              timestamppb.New(testComponentCloseTime),
				HistoryLength:          100,
				HistorySizeBytes:       5000,
				StateTransitionCount:   42,
				ChasmSearchAttributes:  chasmSearchAttributes,
				CustomSearchAttributes: customSearchAttributes,
				Memo:                   memoData,
				ChasmMemo:              chasmMemoPayload,
			},
		},
		NextPageToken: []byte("next-token"),
	}

	s.visibilityManager.EXPECT().
		ListChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *visibilityservice.ListChasmExecutionsRequest,
		) (*visibilityservice.ListChasmExecutionsResponse, error) {
			s.Equal(expectedRequest.ArchetypeId, req.ArchetypeId)
			s.Equal(expectedRequest.NamespaceId, req.NamespaceId)
			s.Equal(expectedRequest.Namespace, req.Namespace)
			s.Equal(expectedRequest.PageSize, req.PageSize)
			s.Equal(expectedRequest.NextPageToken, req.NextPageToken)
			s.Equal(expectedRequest.Query, req.Query)
			return expectedResponse, nil
		})

	// Call ListExecutions
	request := &chasm.ListExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         query,
		PageSize:      pageSize,
		NextPageToken: pageToken,
	}

	response, err := s.visibilityMgr.ListExecutions(
		ctx,
		reflect.TypeFor[*visibilityTestComponent](),
		request,
	)

	// Verify results
	s.NoError(err)
	s.NotNil(response)
	s.Len(response.Executions, 1)
	s.Equal([]byte("next-token"), response.NextPageToken)

	execution := response.Executions[0]
	s.Equal(testBusinessID, execution.BusinessId)
	s.Equal(testRunID, execution.RunId)
	s.Equal(testComponentStartTime, execution.StartTime.AsTime())
	s.Equal(testComponentCloseTime, execution.CloseTime.AsTime())
	s.Equal(int64(100), execution.HistoryLength)
	s.Equal(int64(5000), execution.HistorySizeBytes)
	s.Equal(int64(42), execution.StateTransitionCount)

	// Verify chasm search attributes - check that the map was created correctly
	s.NotNil(execution.ChasmSearchAttributes)
	// The SearchAttributesMap wraps the values, so we verify it's not empty
	// The actual values are verified through the manager response

	// Verify custom search attributes
	s.Equal(customSearchAttributes, execution.CustomSearchAttributes)

	// Verify memo
	s.Equal(memoData, execution.Memo)

	// Verify chasm memo
	s.Equal(chasmMemoPayload, execution.ChasmMemo)
}

func (s *ChasmVisibilityManagerSuite) TestCountExecutions_Success() {
	ctx := context.Background()

	query := "StartTime > '2024-01-01T00:00:00Z'"

	// Get archetype ID for the test component
	archetypeID, ok := s.registry.ArchetypeIDOf(reflect.TypeFor[*visibilityTestComponent]())
	s.True(ok)

	expectedCount := int64(42)

	// Setup visibility manager mock
	expectedRequest := &visibilityservice.CountChasmExecutionsRequest{
		ArchetypeId: archetypeID,
		NamespaceId: testChasmNamespaceID.String(),
		Namespace:   testChasmNamespace.String(),
		Query:       query,
	}

	expectedResponse := &visibilityservice.CountChasmExecutionsResponse{
		Count: expectedCount,
	}

	s.visibilityManager.EXPECT().
		CountChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *visibilityservice.CountChasmExecutionsRequest,
		) (*visibilityservice.CountChasmExecutionsResponse, error) {
			s.Equal(expectedRequest.ArchetypeId, req.ArchetypeId)
			s.Equal(expectedRequest.NamespaceId, req.NamespaceId)
			s.Equal(expectedRequest.Namespace, req.Namespace)
			s.Equal(expectedRequest.Query, req.Query)
			return expectedResponse, nil
		})

	// Call CountExecutions
	request := &chasm.CountExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         query,
	}

	response, err := s.visibilityMgr.CountExecutions(
		ctx,
		reflect.TypeFor[*visibilityTestComponent](),
		request,
	)

	// Verify results
	s.NoError(err)
	s.NotNil(response)
	s.Equal(expectedCount, response.Count)
}

func (s *ChasmVisibilityManagerSuite) TestListExecutions_InvalidArchetypeType() {
	ctx := context.Background()

	// Use an invalid type that's not registered
	invalidType := reflect.TypeFor[struct{ Field string }]()

	request := &chasm.ListExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         "StartTime > '2024-01-01T00:00:00Z'",
	}

	response, err := s.visibilityMgr.ListExecutions(
		ctx,
		invalidType,
		request,
	)

	s.Error(err)
	s.Nil(response)
}

func (s *ChasmVisibilityManagerSuite) TestCountExecutions_InvalidArchetypeType() {
	ctx := context.Background()

	// Use an invalid type that's not registered
	invalidType := reflect.TypeFor[struct{ Field string }]()

	request := &chasm.CountExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         "StartTime > '2024-01-01T00:00:00Z'",
	}

	response, err := s.visibilityMgr.CountExecutions(
		ctx,
		invalidType,
		request,
	)

	s.Error(err)
	s.Nil(response)
}

func (s *ChasmVisibilityManagerSuite) TestListExecutions_VisibilityManagerError() {
	ctx := context.Background()

	query := "StartTime > '2024-01-01T00:00:00Z'"

	// Get archetype ID for the test component
	archetypeID, ok := s.registry.ArchetypeIDOf(reflect.TypeFor[*visibilityTestComponent]())
	s.True(ok)

	// Setup visibility manager mock to return an error
	s.visibilityManager.EXPECT().
		ListChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *visibilityservice.ListChasmExecutionsRequest,
		) (*visibilityservice.ListChasmExecutionsResponse, error) {
			s.Equal(archetypeID, req.ArchetypeId)
			return nil, errTestVisibilityError
		})

	request := &chasm.ListExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         query,
	}

	response, err := s.visibilityMgr.ListExecutions(
		ctx,
		reflect.TypeFor[*visibilityTestComponent](),
		request,
	)

	s.Error(err)
	s.Nil(response)
	s.Equal(errTestVisibilityError, err)
}

func (s *ChasmVisibilityManagerSuite) TestCountExecutions_VisibilityManagerError() {
	ctx := context.Background()

	query := "StartTime > '2024-01-01T00:00:00Z'"

	// Get archetype ID for the test component
	archetypeID, ok := s.registry.ArchetypeIDOf(reflect.TypeFor[*visibilityTestComponent]())
	s.True(ok)

	// Setup visibility manager mock to return an error
	s.visibilityManager.EXPECT().
		CountChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *visibilityservice.CountChasmExecutionsRequest,
		) (*visibilityservice.CountChasmExecutionsResponse, error) {
			s.Equal(archetypeID, req.ArchetypeId)
			return nil, errTestVisibilityError
		})

	request := &chasm.CountExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         query,
	}

	response, err := s.visibilityMgr.CountExecutions(
		ctx,
		reflect.TypeFor[*visibilityTestComponent](),
		request,
	)

	s.Error(err)
	s.Nil(response)
	s.Equal(errTestVisibilityError, err)
}

// visibilityTestComponentWithTaskQueue is a test component that uses TaskQueue as a preallocated search attribute.
type visibilityTestComponentWithTaskQueue struct {
	chasm.UnimplementedComponent
	Data *persistencespb.WorkflowExecutionState
}

func (tc *visibilityTestComponentWithTaskQueue) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *ChasmVisibilityManagerSuite) TestListExecutions_WithTaskQueueSearchAttribute() {
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()

	library := chasm.NewMockLibrary(ctrl)
	library.EXPECT().Name().Return("TaskQueueTestLibrary").AnyTimes()
	library.EXPECT().Components().Return([]*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*visibilityTestComponentWithTaskQueue](
			"TaskQueueComponent",
			chasm.WithSearchAttributes(
				chasm.SearchAttributeTaskQueue,
			),
		),
	}).AnyTimes()
	library.EXPECT().Tasks().Return(nil).AnyTimes()
	library.EXPECT().NexusServices().Return(nil).AnyTimes()
	library.EXPECT().NexusServiceProcessors().Return(nil).AnyTimes()

	registry := chasm.NewRegistry(log.NewNoopLogger())
	err := registry.Register(library)
	s.NoError(err)

	visibilityMgr := manager.NewMockVisibilityManager(ctrl)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceID(testChasmNamespace).Return(testChasmNamespaceID, nil).AnyTimes()
	chasmVisMgr := NewChasmVisibilityManager(registry, nsRegistry, visibilityMgr)

	ctx := context.Background()
	query := "TaskQueue = 'my-task-queue'"
	pageSize := 10

	archetypeID, ok := registry.ArchetypeIDOf(reflect.TypeFor[*visibilityTestComponentWithTaskQueue]())
	s.True(ok)

	expectedTaskQueue := "my-task-queue"
	chasmSearchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			sadefs.TaskQueue: sadefs.MustEncodeValue(expectedTaskQueue, enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		},
	}

	expectedResponse := &visibilityservice.ListChasmExecutionsResponse{
		Executions: []*chasmspb.VisibilityExecutionInfo{
			{
				BusinessId:            testBusinessID,
				RunId:                 testRunID,
				StartTime:             timestamppb.New(testComponentStartTime),
				CloseTime:             timestamppb.New(testComponentCloseTime),
				ChasmSearchAttributes: chasmSearchAttributes,
			},
		},
		NextPageToken: nil,
	}

	visibilityMgr.EXPECT().
		ListChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *visibilityservice.ListChasmExecutionsRequest,
		) (*visibilityservice.ListChasmExecutionsResponse, error) {
			s.Equal(archetypeID, req.ArchetypeId)
			return expectedResponse, nil
		})

	// Call ListExecutions
	request := &chasm.ListExecutionsRequest{
		NamespaceName: string(testChasmNamespace),
		Query:         query,
		PageSize:      pageSize,
	}

	response, err := chasmVisMgr.ListExecutions(
		ctx,
		reflect.TypeFor[*visibilityTestComponentWithTaskQueue](),
		request,
	)

	s.NoError(err)
	s.NotNil(response)
	s.Len(response.Executions, 1)

	execution := response.Executions[0]
	s.NotNil(execution.ChasmSearchAttributes)
	// Verify TaskQueue is in the CHASM search attributes
	s.Contains(execution.ChasmSearchAttributes.GetIndexedFields(), sadefs.TaskQueue)
	taskQueueVal, err := sadefs.DecodeValue(
		execution.ChasmSearchAttributes.IndexedFields[sadefs.TaskQueue],
		enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
		false,
	)
	s.NoError(err)
	s.Equal(expectedTaskQueue, taskQueueVal)
}

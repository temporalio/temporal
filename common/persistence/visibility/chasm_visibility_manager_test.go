package visibility

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type (
	ChasmVisibilityManagerSuite struct {
		*require.Assertions
		suite.Suite
		controller *gomock.Controller

		registry          *chasm.Registry
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

	s.registry = chasm.NewRegistry(log.NewNoopLogger())
	err := s.registry.Register(library)
	s.NoError(err)

	s.visibilityManager = manager.NewMockVisibilityManager(s.controller)

	s.visibilityMgr = NewChasmVisibilityManager(
		s.registry,
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
	chasmSearchAttributes := chasm.NewSearchAttributesMap(map[string]chasm.VisibilityValue{
		testChasmSA1Name: chasm.VisibilityValueInt64(123),
		testChasmSA2Name: chasm.VisibilityValueString("test-value"),
	})

	// Create custom search attributes map
	customSearchAttributes := map[string]*commonpb.Payload{
		testCustomSAName: customSAPayload,
	}

	// Setup visibility manager mock
	expectedRequest := &manager.ListChasmExecutionsRequest{
		ArchetypeID:   archetypeID,
		NamespaceID:   testChasmNamespaceID,
		Namespace:     testChasmNamespace,
		PageSize:      pageSize,
		NextPageToken: pageToken,
		Query:         query,
	}

	expectedResponse := &chasm.ListExecutionsResponse[*commonpb.Payload]{
		Executions: []*chasm.ExecutionInfo[*commonpb.Payload]{
			{
				BusinessID:             testBusinessID,
				RunID:                  testRunID,
				StartTime:              testComponentStartTime,
				CloseTime:              testComponentCloseTime,
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
		DoAndReturn(func(_ context.Context, req *manager.ListChasmExecutionsRequest) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
			s.Equal(expectedRequest.ArchetypeID, req.ArchetypeID)
			s.Equal(expectedRequest.NamespaceID, req.NamespaceID)
			s.Equal(expectedRequest.Namespace, req.Namespace)
			s.Equal(expectedRequest.PageSize, req.PageSize)
			s.Equal(expectedRequest.NextPageToken, req.NextPageToken)
			s.Equal(expectedRequest.Query, req.Query)
			return expectedResponse, nil
		})

	// Call ListExecutions
	request := &chasm.ListExecutionsRequest{
		NamespaceID:   string(testChasmNamespaceID),
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
	s.Equal(testBusinessID, execution.BusinessID)
	s.Equal(testRunID, execution.RunID)
	s.Equal(testComponentStartTime, execution.StartTime)
	s.Equal(testComponentCloseTime, execution.CloseTime)
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
	expectedRequest := &manager.CountChasmExecutionsRequest{
		ArchetypeID: archetypeID,
		NamespaceID: testChasmNamespaceID,
		Namespace:   testChasmNamespace,
		Query:       query,
	}

	expectedResponse := &chasm.CountExecutionsResponse{
		Count: expectedCount,
	}

	s.visibilityManager.EXPECT().
		CountChasmExecutions(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, req *manager.CountChasmExecutionsRequest) (*chasm.CountExecutionsResponse, error) {
			s.Equal(expectedRequest.ArchetypeID, req.ArchetypeID)
			s.Equal(expectedRequest.NamespaceID, req.NamespaceID)
			s.Equal(expectedRequest.Namespace, req.Namespace)
			s.Equal(expectedRequest.Query, req.Query)
			return expectedResponse, nil
		})

	// Call CountExecutions
	request := &chasm.CountExecutionsRequest{
		NamespaceID:   string(testChasmNamespaceID),
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
		NamespaceID:   string(testChasmNamespaceID),
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
		NamespaceID:   string(testChasmNamespaceID),
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
		DoAndReturn(func(_ context.Context, req *manager.ListChasmExecutionsRequest) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
			s.Equal(archetypeID, req.ArchetypeID)
			return nil, errTestVisibilityError
		})

	request := &chasm.ListExecutionsRequest{
		NamespaceID:   string(testChasmNamespaceID),
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
		DoAndReturn(func(_ context.Context, req *manager.CountChasmExecutionsRequest) (*chasm.CountExecutionsResponse, error) {
			s.Equal(archetypeID, req.ArchetypeID)
			return nil, errTestVisibilityError
		})

	request := &chasm.CountExecutionsRequest{
		NamespaceID:   string(testChasmNamespaceID),
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

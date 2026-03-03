package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const (
	chasmTestTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

type ChasmTestSuite struct {
	testcore.FunctionalTestBase

	chasmContext                context.Context
	enableUnifiedQueryConverter bool
}

// TODO: Remove enableUnifiedQueryConverter flag once we have migrated to the unified query converter.
// Functional tests will temporarily check both to validate all paths of chasm query implementation.
func TestChasmTestSuiteLegacy(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ChasmTestSuite{enableUnifiedQueryConverter: false})
}

func TestChasmTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ChasmTestSuite{enableUnifiedQueryConverter: true})
}

func (s *ChasmTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                           true,
			dynamicconfig.VisibilityEnableUnifiedQueryConverter.Key(): s.enableUnifiedQueryConverter,
		}),
	)

	chasmEngine, err := s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	chasmVisibilityMgr := s.GetTestCluster().Host().ChasmVisibilityManager()
	s.Require().NotNil(chasmVisibilityMgr)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.chasmContext = chasm.NewVisibilityManagerContext(s.chasmContext, chasmVisibilityMgr)
}

func (s *ChasmTestSuite) TestNewPayloadStore() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          tv.Any().String(),
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)
}

func (s *ChasmTestSuite) TestNewPayloadStore_ConflictPolicy_UseExisting() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	resp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	currentRunID := resp.RunID

	resp, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))

	resp, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyUseExisting,
		},
	)
	s.NoError(err)
	s.Equal(currentRunID, resp.RunID)
}

func (s *ChasmTestSuite) TestPayloadStore_UpdateComponent() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
		},
	)
	s.NoError(err)

	descResp, err := tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.Equal(int64(1), descResp.State.TotalCount)
	s.Positive(descResp.State.TotalSize)
}

func (s *ChasmTestSuite) TestPayloadStore_PureTask() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
			TTL:         1 * time.Second,
		},
	)
	s.NoError(err)

	s.Eventually(func() bool {
		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
		return descResp.State.TotalCount == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *ChasmTestSuite) TestListExecutions() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)

	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND PayloadStoreId = '%s'", archetypeID, storeID)

	var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(storeID, visRecord.BusinessID)
	s.Equal(createResp.RunID, visRecord.RunID)
	s.NotEmpty(visRecord.StartTime)
	s.Empty(visRecord.StateTransitionCount)

	totalCount := visRecord.ChasmMemo.TotalCount
	s.Equal(0, int(totalCount))
	totalSize := visRecord.ChasmMemo.TotalSize
	s.Equal(0, int(totalSize))
	totalCountSA, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalCountSearchAttribute)
	s.True(ok)
	s.Equal(0, int(totalCountSA))
	totalSizeSA, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalSizeSearchAttribute)
	s.True(ok)
	s.Equal(0, int(totalSizeSA))
	var scheduledByID string
	s.NoError(payload.Decode(visRecord.CustomSearchAttributes[sadefs.TemporalScheduledById], &scheduledByID))
	s.Equal(tests.TestScheduleID, scheduledByID)
	var archetypeIDStr string
	s.NoError(payload.Decode(visRecord.CustomSearchAttributes[sadefs.TemporalNamespaceDivision], &archetypeIDStr))
	parsedArchetypeID, err := strconv.ParseUint(archetypeIDStr, 10, 32)
	s.NoError(err)
	s.Equal(archetypeID, chasm.ArchetypeID(parsedArchetypeID))

	addPayloadResp, err := tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
		},
	)
	s.NoError(err)

	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery + " AND PayloadTotalCount > 0",
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			return visRecord.ChasmMemo.TotalCount == addPayloadResp.State.TotalCount
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	// We validated Count memo field above, just checking for size here.
	s.Equal(addPayloadResp.State.TotalSize, visRecord.ChasmMemo.TotalSize)

	_, err = tests.ClosePayloadStoreHandler(
		ctx,
		tests.ClosePayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   s.NamespaceID().String(),
				NamespaceName: s.Namespace().String(),
				PageSize:      10,
				Query:         visQuery + " AND ExecutionStatus = 'Completed' AND PayloadTotalCount > 0",
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(int64(3), visRecord.StateTransitionCount)
	s.NotEmpty(visRecord.CloseTime)
	s.Empty(visRecord.HistoryLength)
}

func (s *ChasmTestSuite) TestCountExecutions_GroupBy() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	for range 3 {
		storeID := tv.Any().String()

		_, err := tests.NewPayloadStoreHandler(
			s.chasmContext,
			tests.NewPayloadStoreRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
	}

	for range 2 {
		storeID := tv.Any().String()

		resp, err := tests.NewPayloadStoreHandler(
			s.chasmContext,
			tests.NewPayloadStoreRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)

		_, err = tests.AddPayloadHandler(
			s.chasmContext,
			tests.AddPayloadRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "key1",
				Payload:     payload.EncodeString("value1"),
			},
		)
		s.NoError(err)

		_, err = tests.CancelPayloadStoreHandler(
			s.chasmContext,
			tests.CancelPayloadStoreRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
		s.NotEmpty(resp.RunID)
	}

	var countResp *chasm.CountExecutionsResponse
	var err error
	s.Eventually(
		func() bool {
			countResp, err = chasm.CountExecutions[*tests.PayloadStore](
				ctx,
				&chasm.CountExecutionsRequest{
					NamespaceID:   string(s.NamespaceID()),
					NamespaceName: s.Namespace().String(),
					Query:         "GROUP BY `ExecutionStatus`",
				},
			)
			return err == nil && countResp != nil && countResp.Count >= 5
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	s.NoError(err)
	s.NotNil(countResp)
	s.Equal(int64(5), countResp.Count)
	s.Len(countResp.Groups, 2)

	var totalCount int64
	for _, group := range countResp.Groups {
		s.Len(group.Values, 1)
		totalCount += group.Count
		var groupValue string
		s.NoError(payload.Decode(group.Values[0], &groupValue))
		s.Contains([]string{"Running", "Canceled"}, groupValue)
	}
	s.Equal(int64(5), totalCount)

	// Test that GROUP BY on unsupported field returns error
	_, err = chasm.CountExecutions[*tests.PayloadStore](
		ctx,
		&chasm.CountExecutionsRequest{
			NamespaceID:   string(s.NamespaceID()),
			NamespaceName: s.Namespace().String(),
			Query:         "GROUP BY `PayloadTotalCount`",
		},
	)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.Contains(err.Error(), "GROUP BY")
}

func (s *ChasmTestSuite) TestListWorkflowExecutions() {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "test-key",
			Payload:     payload.EncodeString("test-value"),
		},
	)
	s.NoError(err)

	visQuery := sadefs.QueryWithAnyNamespaceDivision(
		fmt.Sprintf("WorkflowId = '%s'", storeID),
	)

	var execInfo *workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			listResp, err := s.FrontendClient().ListWorkflowExecutions(testcore.NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     visQuery,
			})
			s.NoError(err)
			if len(listResp.Executions) != 1 {
				return false
			}
			execInfo = listResp.Executions[0]
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	s.Equal(storeID, execInfo.Execution.WorkflowId)
	s.Equal(createResp.RunID, execInfo.Execution.RunId)

	s.NotNil(execInfo.SearchAttributes)
	_, hasScheduledByID := execInfo.SearchAttributes.IndexedFields[sadefs.TemporalScheduledById]
	s.True(hasScheduledByID)

	_, hasTotalCount := execInfo.SearchAttributes.IndexedFields["TemporalInt01"]
	s.False(hasTotalCount, "CHASM search attribute TemporalInt01 should not be exposed")
	_, hasTotalSize := execInfo.SearchAttributes.IndexedFields["TemporalInt02"]
	s.False(hasTotalSize, "CHASM search attribute TemporalInt02 should not be exposed")
}

func (s *ChasmTestSuite) TestPayloadStoreForceDelete() {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	// Make sure visibility record is created, so that we can test its deletion later.
	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", archetypeID, storeID)
	var executionInfo *workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			resp, err := s.FrontendClient().ListWorkflowExecutions(testcore.NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     visQuery,
			})
			s.NoError(err)
			if len(resp.Executions) > 0 {
				executionInfo = resp.Executions[0]
			}
			return len(resp.Executions) == 1
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	archetypePayload, ok := executionInfo.SearchAttributes.GetIndexedFields()[sadefs.TemporalNamespaceDivision]
	s.True(ok)
	var archetypeIDStr string
	s.NoError(payload.Decode(archetypePayload, &archetypeIDStr))
	parsedArchetypeID, err := strconv.ParseUint(archetypeIDStr, 10, 32)
	s.NoError(err)
	s.Equal(archetypeID, chasm.ArchetypeID(parsedArchetypeID))

	archetype, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentFqnByID(archetypeID)
	s.True(ok)
	_, err = s.AdminClient().DeleteWorkflowExecution(testcore.NewContext(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      createResp.RunID,
		},
		Archetype: archetype,
	})
	s.NoError(err)

	// Validate mutable state is deleted.
	_, err = s.AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      createResp.RunID,
		},
		Archetype: archetype,
	})
	var notFoundErr *serviceerror.NotFound
	s.ErrorAs(err, &notFoundErr)

	// Validate visibility record is deleted.
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   s.NamespaceID().String(),
				NamespaceName: s.Namespace().String(),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			return len(resp.Executions) == 0
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *ChasmTestSuite) TestListExecutions_ExecutionStatusAsAlias() {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)

	// Query using "ExecutionStatus" as a CHASM alias (which maps to TemporalKeyword03)
	// This tests that CHASM components can use "ExecutionStatus" as an alias for their own search attribute
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND PayloadStoreId = '%s'", archetypeID, storeID)

	var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(storeID, visRecord.BusinessID)

	// Verify the ExecutionStatus CHASM search attribute is correctly returned
	executionStatus, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.ExecutionStatusSearchAttribute)
	s.True(ok)
	s.Equal("Running", executionStatus)

	_, err = tests.CancelPayloadStoreHandler(
		ctx,
		tests.CancelPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	visQueryCanceled := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Canceled' AND PayloadStoreId = '%s'", archetypeID, storeID)
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQueryCanceled,
			})
			s.NoError(err)
			return len(resp.Executions) == 1
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *ChasmTestSuite) TestTaskQueuePreallocatedSearchAttribute() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)

	// Query using TaskQueue as a CHASM preallocated search attribute
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND TaskQueue = '%s' AND PayloadStoreId = '%s'", archetypeID, tests.DefaultPayloadStoreTaskQueue, storeID)

	var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(storeID, visRecord.BusinessID)

	// Verify TaskQueue is returned as a CHASM search attribute
	taskQueueVal, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, chasm.SearchAttributeTaskQueue)
	s.True(ok)
	s.Equal(tests.DefaultPayloadStoreTaskQueue, taskQueueVal)
}

func (s *ChasmTestSuite) TestMutableStateRebuilder() {
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	// wait for the payload store to be created
	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	s.Equal(archetypeID, chasm.ArchetypeID(archetypeID))
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", archetypeID, storeID)
	var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
	var runID string
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			if len(resp.Executions) != 1 {
				return false
			}

			visRecord = resp.Executions[0]
			runID = visRecord.RunID
			return true
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(storeID, visRecord.BusinessID)

	// payloadStore archetype is not the workflow archetype, should fail the rebuild.
	archetype, _ := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentFqnByID(archetypeID)
	s.NotEqual(archetype, chasm.WorkflowArchetype, "Archetype should not be the workflow archetype")

	_, err = s.AdminClient().RebuildMutableState(testcore.NewContext(), &adminservice.RebuildMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      runID,
		},
	})
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))
}

func (s *ChasmTestSuite) TestUpdateWithStartExecution_UpdateExisting() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	// Create initial PayloadStore.
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyAllowDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)
	originalRunID := createResp.RunID

	// Add a payload to the original store.
	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "original-key",
			Payload:     payload.EncodeString("original-value"),
		},
	)
	s.NoError(err)

	// Call UpdateWithStartExecution - should update existing running execution.
	newFnCalled := false
	updateFnCalled := false
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: s.NamespaceID().String(),
			BusinessID:  storeID,
		},
		func(mutableContext chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			newFnCalled = true
			s.Fail("newFn should not be called when execution exists and is running")
			return nil, nil
		},
		func(store *tests.PayloadStore, mutableContext chasm.MutableContext, _ any) (any, error) {
			updateFnCalled = true
			// Update the store by closing it (marks for close but doesn't actually close yet).
			store.State.Closed = true
			return nil, nil
		},
		nil,
	)
	s.NoError(err)
	s.False(newFnCalled, "newFn should not be called")
	s.True(updateFnCalled, "updateFn should be called")
	s.Equal(originalRunID, result.ExecutionKey.RunID, "RunID should be the same as original")
	s.NotNil(result.ExecutionRef)

	// Verify the store was updated (closed flag set).
	descResp, err := tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.True(descResp.State.Closed, "Store should be marked as closed")
	s.Equal(int64(1), descResp.State.TotalCount) // Original payload still there.
}

func (s *ChasmTestSuite) TestUpdateWithStartExecution_CreateNew() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	// Call UpdateWithStartExecution without creating execution first - should create new.
	newFnCalled := false
	updateFnCalled := false
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: s.NamespaceID().String(),
			BusinessID:  storeID,
		},
		func(mutableContext chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			newFnCalled = true
			store, err := tests.NewPayloadStore(mutableContext)
			return store, err
		},
		func(store *tests.PayloadStore, mutableContext chasm.MutableContext, _ any) (any, error) {
			updateFnCalled = true
			// Apply update to the newly created store (like adding a signal during SignalWithStart).
			store.State.TotalCount = 42
			return nil, nil
		},
		nil,
	)
	s.NoError(err)
	s.True(newFnCalled, "newFn should be called")
	s.True(updateFnCalled, "updateFn should be called after newFn")
	s.NotEmpty(result.ExecutionKey.RunID)
	s.NotNil(result.ExecutionRef)

	// Verify the store was created with the update applied.
	descResp, err := tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.False(descResp.State.Closed, "Store should not be closed")
	s.Equal(int64(42), descResp.State.TotalCount) // Update was applied during creation.
}

// TODO: More tests here...

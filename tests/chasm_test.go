package tests

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	testspb "go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	chasmTestTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

// ChasmSuite runs CHASM functional tests using a pooled dedicated cluster.
type ChasmSuite struct {
	parallelsuite.Suite[*ChasmSuite]
}

func TestChasmSuite(t *testing.T) {
	parallelsuite.Run(t, &ChasmSuite{})
}

// chasmTestEnv bundles a TestEnv with the CHASM engine context derived from it.
type chasmTestEnv struct {
	*testcore.TestEnv
	chasmCtx context.Context
}

// newChasmTestEnv creates a chasmTestEnv backed by a dedicated cluster with
// EnableChasm overridden for the test.
func newChasmTestEnv(suiteContext context.Context, t *testing.T) chasmTestEnv {
	t.Helper()

	// WithDedicatedCluster acquires an exclusive pooled slot — no fresh cluster
	// creation per test, unlike passing startup dynamic config.
	env := testcore.NewEnv(
		t,
		testcore.WithDedicatedCluster(),
		testcore.WithWorkerService("delete namespace workflow"),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.DeleteNamespaceUseChasmDeleteExecution, true),
	)

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(suiteContext)
	require.NoError(t, err)

	return chasmTestEnv{TestEnv: env, chasmCtx: chasmCtx}
}

func (s *ChasmSuite) TestNewPayloadStore() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          tv.Any().String(),
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)
}

func (s *ChasmSuite) TestNewPayloadStore_ConflictPolicy_UseExisting() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	resp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
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
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))

	resp, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyUseExisting,
		},
	)
	s.NoError(err)
	s.Equal(currentRunID, resp.RunID)
}

func (s *ChasmSuite) TestPayloadStore_UpdateComponent() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
		},
	)
	s.NoError(err)

	descResp, err := tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.Equal(int64(1), descResp.State.TotalCount)
	s.Positive(descResp.State.TotalSize)
}

func (s *ChasmSuite) TestPayloadStore_PureTask() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
			TTL:         1 * time.Second,
		},
	)
	s.NoError(err)

	s.AwaitTrue(func() bool {
		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
		return descResp.State.TotalCount == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *ChasmSuite) TestListExecutions() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)

	var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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
	s.Equal(tests.ArchetypeID, chasm.ArchetypeID(parsedArchetypeID))

	addPayloadResp, err := tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
		},
	)
	s.NoError(err)

	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: cenv.Namespace().String(),
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

func (s *ChasmSuite) TestCountExecutions_GroupBy() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	for range 3 {
		storeID := tv.Any().String()

		_, err := tests.NewPayloadStoreHandler(
			cenv.chasmCtx,
			tests.NewPayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
	}

	for range 2 {
		storeID := tv.Any().String()

		resp, err := tests.NewPayloadStoreHandler(
			cenv.chasmCtx,
			tests.NewPayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)

		_, err = tests.AddPayloadHandler(
			cenv.chasmCtx,
			tests.AddPayloadRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "key1",
				Payload:     payload.EncodeString("value1"),
			},
		)
		s.NoError(err)

		_, err = tests.CancelPayloadStoreHandler(
			cenv.chasmCtx,
			tests.CancelPayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
		s.NotEmpty(resp.RunID)
	}

	var countResp *chasm.CountExecutionsResponse
	var err error
	s.AwaitTrue(
		func() bool {
			countResp, err = chasm.CountExecutions[*tests.PayloadStore](
				ctx,
				&chasm.CountExecutionsRequest{
					NamespaceName: cenv.Namespace().String(),
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
			NamespaceName: cenv.Namespace().String(),
			Query:         "GROUP BY `PayloadTotalCount`",
		},
	)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.Contains(err.Error(), "GROUP BY")
}

func (s *ChasmSuite) TestListWorkflowExecutions() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: cenv.NamespaceID(),
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
	s.AwaitTrue(
		func() bool {
			listResp, err := cenv.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: cenv.Namespace().String(),
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

func (s *ChasmSuite) TestPayloadStoreForceDelete() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	// Make sure visibility record is created, so that we can test its deletion later.
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", tests.ArchetypeID, storeID)
	var executionInfo *workflowpb.WorkflowExecutionInfo
	s.AwaitTrue(
		func() bool {
			resp, err := cenv.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: cenv.Namespace().String(),
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
	s.Equal(tests.ArchetypeID, chasm.ArchetypeID(parsedArchetypeID))

	_, err = cenv.AdminClient().DeleteWorkflowExecution(s.Context(), &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: cenv.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      createResp.RunID,
		},
		Archetype: tests.Archetype,
	})
	s.NoError(err)

	// Validate mutable state is deleted.
	_, err = cenv.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: cenv.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      createResp.RunID,
		},
		Archetype: tests.Archetype,
	})
	var notFoundErr *serviceerror.NotFound
	s.ErrorAs(err, &notFoundErr)

	// Validate visibility record is deleted.
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: cenv.Namespace().String(),
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

func (s *ChasmSuite) TestDeletePayloadStore_RunningExecution() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	visQuery := fmt.Sprintf("WorkflowId = '%s'", storeID)

	// Wait for visibility record to appear.
	s.Await(
		func(s *ChasmSuite) {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: cenv.Namespace().String(),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			s.Len(resp.Executions, 1)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	err = tests.DeletePayloadStoreHandler(
		ctx,
		tests.DeletePayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
			Reason:      "test deletion",
			Identity:    "test-identity",
		},
	)
	s.NoError(err)

	// Validate execution is fully deleted (both mutable state and visibility record).
	s.Await(
		func(s *ChasmSuite) {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: cenv.Namespace().String(),
				PageSize:      10,
				Query:         visQuery,
			})
			s.NoError(err)
			s.Empty(resp.Executions)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *ChasmSuite) TestListExecutions_ExecutionStatusAsAlias() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	// Query using "ExecutionStatus" as a CHASM alias (which maps to TemporalKeyword03).
	// This tests that CHASM components can use "ExecutionStatus" as an alias for their own search attribute.
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)

	var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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

	// Verify the ExecutionStatus CHASM search attribute is correctly returned.
	executionStatus, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.ExecutionStatusSearchAttribute)
	s.True(ok)
	s.Equal("Running", executionStatus)

	_, err = tests.CancelPayloadStoreHandler(
		ctx,
		tests.CancelPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	visQueryCanceled := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Canceled' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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

func (s *ChasmSuite) TestTaskQueuePreallocatedSearchAttribute() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	// Query using TaskQueue as a CHASM preallocated search attribute.
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND TaskQueue = '%s' AND PayloadStoreId = '%s'", tests.ArchetypeID, tests.DefaultPayloadStoreTaskQueue, storeID)

	var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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

	// Verify TaskQueue is returned as a CHASM search attribute.
	taskQueueVal, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, chasm.SearchAttributeTaskQueue)
	s.True(ok)
	s.Equal(tests.DefaultPayloadStoreTaskQueue, taskQueueVal)
}

func (s *ChasmSuite) TestMutableStateRebuilder() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())
	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	// Wait for the payload store to be visible.
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", tests.ArchetypeID, storeID)
	var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
	var runID string
	s.AwaitTrue(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
				NamespaceName: string(cenv.Namespace()),
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
	s.NotEqual(tests.Archetype, chasm.WorkflowArchetype, "Archetype should not be the workflow archetype")

	_, err = cenv.AdminClient().RebuildMutableState(s.Context(), &adminservice.RebuildMutableStateRequest{
		Namespace: cenv.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
			RunId:      runID,
		},
	})
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))
}

func (s *ChasmSuite) TestUpdateWithStartExecution_UpdateExisting() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	// Create initial PayloadStore.
	createResp, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      cenv.NamespaceID(),
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
			NamespaceID: cenv.NamespaceID(),
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
			NamespaceID: cenv.NamespaceID().String(),
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
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.True(descResp.State.Closed, "Store should be marked as closed")
	s.Equal(int64(1), descResp.State.TotalCount) // Original payload still there.
}

func (s *ChasmSuite) TestUpdateWithStartExecution_CreateNew() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	// Call UpdateWithStartExecution without creating execution first - should create new.
	newFnCalled := false
	updateFnCalled := false
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: cenv.NamespaceID().String(),
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
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	s.False(descResp.State.Closed, "Store should not be closed")
	s.Equal(int64(42), descResp.State.TotalCount) // Update was applied during creation.
}

func (s *ChasmSuite) TestPayloadStore_ApproximateExecutionSize() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(cenv.chasmCtx, chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	descResp, err := tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	initialApproxSize := descResp.ApproximateStateSize

	payloadSize := 100 * 1024 // 100KB
	payloadData := make([]byte, payloadSize)
	_, err = rand.Read(payloadData)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		ctx,
		tests.AddPayloadRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeBytes(payloadData),
		},
	)
	s.NoError(err)

	descResp, err = tests.DescribePayloadStoreHandler(
		ctx,
		tests.DescribePayloadStoreRequest{
			NamespaceID: cenv.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)
	currentApproxSize := descResp.ApproximateStateSize
	sizeDelta := float64(100) // Allow 100 bytes of variance due to overhead, encoding, etc.
	s.InDelta(payloadSize, currentApproxSize-initialApproxSize, sizeDelta)

	adminDescResp, err := cenv.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: cenv.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
		},
		ArchetypeId: tests.ArchetypeID,
	})
	s.NoError(err)
	s.InDelta(adminDescResp.DatabaseMutableState.Size(), currentApproxSize, sizeDelta)
}

// TestNamespaceDelete_WithChasmExecutions verifies that running CHASM executions are cleaned
// up when their namespace is deleted, exercising the DeleteExecution history service API.
func (s *ChasmSuite) TestNamespaceDelete_WithChasmExecutions() {
	cenv := newChasmTestEnv(s.Context(), s.T())
	tv := testvars.New(s.T())

	// Register a fresh namespace for this test.
	var namespaceSuffix [4]byte
	_, err := rand.Read(namespaceSuffix[:])
	s.NoError(err)
	namespaceName := fmt.Sprintf("ns-chasm-delete-%x", namespaceSuffix)
	_, err = cenv.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespaceName,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := cenv.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespaceName,
	})
	s.NoError(err)
	nsID := namespace.ID(descResp.GetNamespaceInfo().GetId())

	// Create running CHASM executions in the new namespace.
	const numExecutions = 3
	for range numExecutions {
		_, err = tests.NewPayloadStoreHandler(cenv.chasmCtx, tests.NewPayloadStoreRequest{
			NamespaceID:      nsID,
			StoreID:          tv.Any().String(),
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		})
		s.NoError(err)
	}

	// Wait for visibility records to appear.
	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d'", tests.ArchetypeID)
	await.Require(s.Context(), s.T(), func(t *await.T) {
		resp, err := cenv.FrontendClient().ListWorkflowExecutions(t.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: namespaceName,
			PageSize:  10,
			Query:     visQuery,
		})
		require.NoError(t, err)
		require.Len(t, resp.Executions, numExecutions)
	}, testcore.WaitForESToSettle, 100*time.Millisecond)

	// Delete the namespace, which should trigger DeleteExecution for all CHASM executions.
	_, err = cenv.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace: namespaceName,
	})
	s.NoError(err)

	// Verify all CHASM executions are cleaned up from visibility.
	await.Require(s.Context(), s.T(), func(t *await.T) {
		resp, err := cenv.FrontendClient().ListWorkflowExecutions(t.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: namespaceName,
			PageSize:  10,
			Query:     visQuery,
		})
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return // namespace fully deleted is also acceptable
		}
		require.NoError(t, err)
		require.Empty(t, resp.Executions)
	}, 20*time.Second*debug.TimeoutMultiplier, time.Second)
}

// TODO: More tests here...

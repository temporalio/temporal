package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	testspb "go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const (
	chasmTestTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

// ChasmSuite runs CHASM functional tests using a pooled dedicated cluster.
// Each test is exercised with both the legacy and unified visibility query converters
// via s.Run subtests named "unified=false" and "unified=true".
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
// EnableChasm and VisibilityEnableUnifiedQueryConverter overridden for the test.
func newChasmTestEnv(t *testing.T, unified bool) chasmTestEnv {
	t.Helper()

	// WithDedicatedCluster acquires an exclusive pooled slot — no fresh cluster
	// creation per test, unlike passing startup dynamic config.
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.VisibilityEnableUnifiedQueryConverter, unified)

	chasmEngine, err := env.GetTestCluster().Host().ChasmEngine()
	require.NoError(t, err)
	require.NotNil(t, chasmEngine)

	chasmVisibilityMgr := env.GetTestCluster().Host().ChasmVisibilityManager()
	require.NotNil(t, chasmVisibilityMgr)

	chasmCtx := chasm.NewEngineContext(env.Context(), chasmEngine)
	chasmCtx = chasm.NewVisibilityManagerContext(chasmCtx, chasmVisibilityMgr)

	return chasmTestEnv{TestEnv: env, chasmCtx: chasmCtx}
}

// forBothConverters runs fn as two parallel subtests, one with the legacy visibility
// query converter and one with the unified converter.
// TODO: Remove once we have fully migrated to the unified query converter.
func (s *ChasmSuite) forBothConverters(fn func(*ChasmSuite, chasmTestEnv)) {
	for _, unified := range []bool{false, true} {
		unified := unified
		s.Run(fmt.Sprintf("unified=%v", unified), func(ss *ChasmSuite) {
			fn(ss, newChasmTestEnv(ss.T(), unified))
		})
	}
}

func (s *ChasmSuite) TestNewPayloadStore() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)
	})
}

func (s *ChasmSuite) TestNewPayloadStore_ConflictPolicy_UseExisting() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

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
		ss.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))

		resp, err = tests.NewPayloadStoreHandler(
			ctx,
			tests.NewPayloadStoreRequest{
				NamespaceID:      cenv.NamespaceID(),
				StoreID:          storeID,
				IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
				IDConflictPolicy: chasm.BusinessIDConflictPolicyUseExisting,
			},
		)
		ss.NoError(err)
		ss.Equal(currentRunID, resp.RunID)
	})
}

func (s *ChasmSuite) TestPayloadStore_UpdateComponent() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

		_, err = tests.AddPayloadHandler(
			ctx,
			tests.AddPayloadRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "key1",
				Payload:     payload.EncodeString("value1"),
			},
		)
		ss.NoError(err)

		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)
		ss.Equal(int64(1), descResp.State.TotalCount)
		ss.Positive(descResp.State.TotalSize)
	})
}

func (s *ChasmSuite) TestPayloadStore_PureTask() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

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
		ss.NoError(err)

		ss.Eventually(func() bool {
			descResp, err := tests.DescribePayloadStoreHandler(
				ctx,
				tests.DescribePayloadStoreRequest{
					NamespaceID: cenv.NamespaceID(),
					StoreID:     storeID,
				},
			)
			ss.NoError(err)
			return descResp.State.TotalCount == 0
		}, 10*time.Second, 100*time.Millisecond)
	})
}

func (s *ChasmSuite) TestListExecutions() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

		visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)

		var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQuery,
				})
				ss.NoError(err)
				if len(resp.Executions) != 1 {
					return false
				}

				visRecord = resp.Executions[0]
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		ss.Equal(storeID, visRecord.BusinessID)
		ss.Equal(createResp.RunID, visRecord.RunID)
		ss.NotEmpty(visRecord.StartTime)
		ss.Empty(visRecord.StateTransitionCount)

		totalCount := visRecord.ChasmMemo.TotalCount
		ss.Equal(0, int(totalCount))
		totalSize := visRecord.ChasmMemo.TotalSize
		ss.Equal(0, int(totalSize))
		totalCountSA, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalCountSearchAttribute)
		ss.True(ok)
		ss.Equal(0, int(totalCountSA))
		totalSizeSA, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalSizeSearchAttribute)
		ss.True(ok)
		ss.Equal(0, int(totalSizeSA))
		var scheduledByID string
		ss.NoError(payload.Decode(visRecord.CustomSearchAttributes[sadefs.TemporalScheduledById], &scheduledByID))
		ss.Equal(tests.TestScheduleID, scheduledByID)
		var archetypeIDStr string
		ss.NoError(payload.Decode(visRecord.CustomSearchAttributes[sadefs.TemporalNamespaceDivision], &archetypeIDStr))
		parsedArchetypeID, err := strconv.ParseUint(archetypeIDStr, 10, 32)
		ss.NoError(err)
		ss.Equal(tests.ArchetypeID, chasm.ArchetypeID(parsedArchetypeID))

		addPayloadResp, err := tests.AddPayloadHandler(
			ctx,
			tests.AddPayloadRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "key1",
				Payload:     payload.EncodeString("value1"),
			},
		)
		ss.NoError(err)

		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQuery + " AND PayloadTotalCount > 0",
				})
				ss.NoError(err)
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
		ss.Equal(addPayloadResp.State.TotalSize, visRecord.ChasmMemo.TotalSize)

		_, err = tests.ClosePayloadStoreHandler(
			ctx,
			tests.ClosePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)

		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: cenv.Namespace().String(),
					PageSize:      10,
					Query:         visQuery + " AND ExecutionStatus = 'Completed' AND PayloadTotalCount > 0",
				})
				ss.NoError(err)
				if len(resp.Executions) != 1 {
					return false
				}

				visRecord = resp.Executions[0]
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		ss.Equal(int64(3), visRecord.StateTransitionCount)
		ss.NotEmpty(visRecord.CloseTime)
		ss.Empty(visRecord.HistoryLength)
	})
}

func (s *ChasmSuite) TestCountExecutions_GroupBy() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
			ss.NoError(err)
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
			ss.NoError(err)

			_, err = tests.AddPayloadHandler(
				cenv.chasmCtx,
				tests.AddPayloadRequest{
					NamespaceID: cenv.NamespaceID(),
					StoreID:     storeID,
					PayloadKey:  "key1",
					Payload:     payload.EncodeString("value1"),
				},
			)
			ss.NoError(err)

			_, err = tests.CancelPayloadStoreHandler(
				cenv.chasmCtx,
				tests.CancelPayloadStoreRequest{
					NamespaceID: cenv.NamespaceID(),
					StoreID:     storeID,
				},
			)
			ss.NoError(err)
			ss.NotEmpty(resp.RunID)
		}

		var countResp *chasm.CountExecutionsResponse
		var err error
		ss.Eventually(
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

		ss.NoError(err)
		ss.NotNil(countResp)
		ss.Equal(int64(5), countResp.Count)
		ss.Len(countResp.Groups, 2)

		var totalCount int64
		for _, group := range countResp.Groups {
			ss.Len(group.Values, 1)
			totalCount += group.Count
			var groupValue string
			ss.NoError(payload.Decode(group.Values[0], &groupValue))
			ss.Contains([]string{"Running", "Canceled"}, groupValue)
		}
		ss.Equal(int64(5), totalCount)

		// Test that GROUP BY on unsupported field returns error
		_, err = chasm.CountExecutions[*tests.PayloadStore](
			ctx,
			&chasm.CountExecutionsRequest{
				NamespaceName: cenv.Namespace().String(),
				Query:         "GROUP BY `PayloadTotalCount`",
			},
		)
		var invalidArgument *serviceerror.InvalidArgument
		ss.ErrorAs(err, &invalidArgument)
		ss.Contains(err.Error(), "GROUP BY")
	})
}

func (s *ChasmSuite) TestListWorkflowExecutions() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())
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
		ss.NoError(err)

		_, err = tests.AddPayloadHandler(
			ctx,
			tests.AddPayloadRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "test-key",
				Payload:     payload.EncodeString("test-value"),
			},
		)
		ss.NoError(err)

		visQuery := sadefs.QueryWithAnyNamespaceDivision(
			fmt.Sprintf("WorkflowId = '%s'", storeID),
		)

		var execInfo *workflowpb.WorkflowExecutionInfo
		ss.Eventually(
			func() bool {
				listResp, err := cenv.FrontendClient().ListWorkflowExecutions(cenv.Context(), &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: cenv.Namespace().String(),
					PageSize:  10,
					Query:     visQuery,
				})
				ss.NoError(err)
				if len(listResp.Executions) != 1 {
					return false
				}
				execInfo = listResp.Executions[0]
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)

		ss.Equal(storeID, execInfo.Execution.WorkflowId)
		ss.Equal(createResp.RunID, execInfo.Execution.RunId)

		ss.NotNil(execInfo.SearchAttributes)
		_, hasScheduledByID := execInfo.SearchAttributes.IndexedFields[sadefs.TemporalScheduledById]
		ss.True(hasScheduledByID)

		_, hasTotalCount := execInfo.SearchAttributes.IndexedFields["TemporalInt01"]
		ss.False(hasTotalCount, "CHASM search attribute TemporalInt01 should not be exposed")
		_, hasTotalSize := execInfo.SearchAttributes.IndexedFields["TemporalInt02"]
		ss.False(hasTotalSize, "CHASM search attribute TemporalInt02 should not be exposed")
	})
}

func (s *ChasmSuite) TestPayloadStoreForceDelete() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())
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
		ss.NoError(err)

		// Make sure visibility record is created, so that we can test its deletion later.
		visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", tests.ArchetypeID, storeID)
		var executionInfo *workflowpb.WorkflowExecutionInfo
		ss.Eventually(
			func() bool {
				resp, err := cenv.FrontendClient().ListWorkflowExecutions(cenv.Context(), &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: cenv.Namespace().String(),
					PageSize:  10,
					Query:     visQuery,
				})
				ss.NoError(err)
				if len(resp.Executions) > 0 {
					executionInfo = resp.Executions[0]
				}
				return len(resp.Executions) == 1
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		archetypePayload, ok := executionInfo.SearchAttributes.GetIndexedFields()[sadefs.TemporalNamespaceDivision]
		ss.True(ok)
		var archetypeIDStr string
		ss.NoError(payload.Decode(archetypePayload, &archetypeIDStr))
		parsedArchetypeID, err := strconv.ParseUint(archetypeIDStr, 10, 32)
		ss.NoError(err)
		ss.Equal(tests.ArchetypeID, chasm.ArchetypeID(parsedArchetypeID))

		_, err = cenv.AdminClient().DeleteWorkflowExecution(cenv.Context(), &adminservice.DeleteWorkflowExecutionRequest{
			Namespace: cenv.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: storeID,
				RunId:      createResp.RunID,
			},
			Archetype: tests.Archetype,
		})
		ss.NoError(err)

		// Validate mutable state is deleted.
		_, err = cenv.AdminClient().DescribeMutableState(cenv.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: cenv.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: storeID,
				RunId:      createResp.RunID,
			},
			Archetype: tests.Archetype,
		})
		var notFoundErr *serviceerror.NotFound
		ss.ErrorAs(err, &notFoundErr)

		// Validate visibility record is deleted.
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: cenv.Namespace().String(),
					PageSize:      10,
					Query:         visQuery,
				})
				ss.NoError(err)
				return len(resp.Executions) == 0
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})
}

func (s *ChasmSuite) TestDeletePayloadStore_RunningExecution() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())
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
		ss.NoError(err)

		visQuery := fmt.Sprintf("WorkflowId = '%s'", storeID)

		// Wait for visibility record to appear.
		ss.EventuallyWithT(
			func(t *assert.CollectT) {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: cenv.Namespace().String(),
					PageSize:      10,
					Query:         visQuery,
				})
				require.NoError(t, err)
				require.Len(t, resp.Executions, 1)
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
		ss.NoError(err)

		// Validate execution is fully deleted (both mutable state and visibility record).
		ss.EventuallyWithT(
			func(t *assert.CollectT) {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: cenv.Namespace().String(),
					PageSize:      10,
					Query:         visQuery,
				})
				require.NoError(t, err)
				require.Empty(t, resp.Executions)
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})
}

func (s *ChasmSuite) TestListExecutions_ExecutionStatusAsAlias() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())
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
		ss.NoError(err)

		// Query using "ExecutionStatus" as a CHASM alias (which maps to TemporalKeyword03).
		// This tests that CHASM components can use "ExecutionStatus" as an alias for their own search attribute.
		visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Running' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)

		var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQuery,
				})
				ss.NoError(err)
				if len(resp.Executions) != 1 {
					return false
				}

				visRecord = resp.Executions[0]
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		ss.Equal(storeID, visRecord.BusinessID)

		// Verify the ExecutionStatus CHASM search attribute is correctly returned.
		executionStatus, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, tests.ExecutionStatusSearchAttribute)
		ss.True(ok)
		ss.Equal("Running", executionStatus)

		_, err = tests.CancelPayloadStoreHandler(
			ctx,
			tests.CancelPayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)

		visQueryCanceled := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND ExecutionStatus = 'Canceled' AND PayloadStoreId = '%s'", tests.ArchetypeID, storeID)
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQueryCanceled,
				})
				ss.NoError(err)
				return len(resp.Executions) == 1
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	})
}

func (s *ChasmSuite) TestTaskQueuePreallocatedSearchAttribute() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

		// Query using TaskQueue as a CHASM preallocated search attribute.
		visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND TaskQueue = '%s' AND PayloadStoreId = '%s'", tests.ArchetypeID, tests.DefaultPayloadStoreTaskQueue, storeID)

		var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQuery,
				})
				ss.NoError(err)
				if len(resp.Executions) != 1 {
					return false
				}

				visRecord = resp.Executions[0]
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
		ss.Equal(storeID, visRecord.BusinessID)

		// Verify TaskQueue is returned as a CHASM search attribute.
		taskQueueVal, ok := chasm.SearchAttributeValue(visRecord.ChasmSearchAttributes, chasm.SearchAttributeTaskQueue)
		ss.True(ok)
		ss.Equal(tests.DefaultPayloadStoreTaskQueue, taskQueueVal)
	})
}

func (s *ChasmSuite) TestMutableStateRebuilder() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())
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
		ss.NoError(err)

		// Wait for the payload store to be visible.
		visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", tests.ArchetypeID, storeID)
		var visRecord *chasm.VisibilityExecutionInfo[*testspb.TestPayloadStore]
		var runID string
		ss.Eventually(
			func() bool {
				resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](ctx, &chasm.ListExecutionsRequest{
					NamespaceName: string(cenv.Namespace()),
					PageSize:      10,
					Query:         visQuery,
				})
				ss.NoError(err)
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
		ss.Equal(storeID, visRecord.BusinessID)

		// payloadStore archetype is not the workflow archetype, should fail the rebuild.
		ss.NotEqual(tests.Archetype, chasm.WorkflowArchetype, "Archetype should not be the workflow archetype")

		_, err = cenv.AdminClient().RebuildMutableState(cenv.Context(), &adminservice.RebuildMutableStateRequest{
			Namespace: cenv.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: storeID,
				RunId:      runID,
			},
		})
		ss.ErrorAs(err, new(*serviceerror.InvalidArgument))
	})
}

func (s *ChasmSuite) TestUpdateWithStartExecution_UpdateExisting() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)
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
		ss.NoError(err)

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
				ss.Fail("newFn should not be called when execution exists and is running")
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
		ss.NoError(err)
		ss.False(newFnCalled, "newFn should not be called")
		ss.True(updateFnCalled, "updateFn should be called")
		ss.Equal(originalRunID, result.ExecutionKey.RunID, "RunID should be the same as original")
		ss.NotNil(result.ExecutionRef)

		// Verify the store was updated (closed flag set).
		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)
		ss.True(descResp.State.Closed, "Store should be marked as closed")
		ss.Equal(int64(1), descResp.State.TotalCount) // Original payload still there.
	})
}

func (s *ChasmSuite) TestUpdateWithStartExecution_CreateNew() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)
		ss.True(newFnCalled, "newFn should be called")
		ss.True(updateFnCalled, "updateFn should be called after newFn")
		ss.NotEmpty(result.ExecutionKey.RunID)
		ss.NotNil(result.ExecutionRef)

		// Verify the store was created with the update applied.
		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)
		ss.False(descResp.State.Closed, "Store should not be closed")
		ss.Equal(int64(42), descResp.State.TotalCount) // Update was applied during creation.
	})
}

func (s *ChasmSuite) TestPayloadStore_ApproximateExecutionSize() {
	s.forBothConverters(func(ss *ChasmSuite, cenv chasmTestEnv) {
		tv := testvars.New(ss.T())

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
		ss.NoError(err)

		descResp, err := tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)
		initialApproxSize := descResp.ApproximateStateSize

		payloadSize := 100 * 1024 // 100KB
		payloadData := make([]byte, payloadSize)
		_, err = rand.Read(payloadData)
		ss.NoError(err)

		_, err = tests.AddPayloadHandler(
			ctx,
			tests.AddPayloadRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
				PayloadKey:  "key1",
				Payload:     payload.EncodeBytes(payloadData),
			},
		)
		ss.NoError(err)

		descResp, err = tests.DescribePayloadStoreHandler(
			ctx,
			tests.DescribePayloadStoreRequest{
				NamespaceID: cenv.NamespaceID(),
				StoreID:     storeID,
			},
		)
		ss.NoError(err)
		currentApproxSize := descResp.ApproximateStateSize
		sizeDelta := float64(100) // Allow 100 bytes of variance due to overhead, encoding, etc.
		ss.InDelta(payloadSize, currentApproxSize-initialApproxSize, sizeDelta)

		adminDescResp, err := cenv.AdminClient().DescribeMutableState(cenv.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: cenv.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: storeID,
			},
			ArchetypeId: tests.ArchetypeID,
		})
		ss.NoError(err)
		ss.InDelta(adminDescResp.DatabaseMutableState.Size(), currentApproxSize, sizeDelta)
	})
}

// TODO: More tests here...

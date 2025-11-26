package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
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

	chasmEngine                 chasm.Engine
	enableUnifiedQueryConverter bool
}

// TODO: Remove enableUnifiedQueryConverter flag once we have migrated to the unified query converter.
// Functional tests will temporarily check both to validate all paths of chasm query implementation.
func TestChasmTestSuiteLegacy(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ChasmTestSuite{enableUnifiedQueryConverter: false})
}

func TestChasmTestSuiteUnifiedQueryConverter(t *testing.T) {
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

	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *ChasmTestSuite) TestNewPayloadStore() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	_, err := tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
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

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()

	resp, err := tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
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
		chasm.NewEngineContext(ctx, s.chasmEngine),
		tests.NewPayloadStoreRequest{
			NamespaceID:      s.NamespaceID(),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.ErrorAs(err, new(*chasm.ExecutionAlreadyStartedError))

	resp, err = tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
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

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
		tests.AddPayloadRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
			PayloadKey:  "key1",
			Payload:     payload.EncodeString("value1"),
		},
	)
	s.NoError(err)

	descResp, err := tests.DescribePayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
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

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	_, err := tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	_, err = tests.AddPayloadHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
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
			chasm.NewEngineContext(ctx, s.chasmEngine),
			tests.DescribePayloadStoreRequest{
				NamespaceID: s.NamespaceID(),
				StoreID:     storeID,
			},
		)
		s.NoError(err)
		return descResp.State.TotalCount == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *ChasmTestSuite) TestPayloadStoreVisibility() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	storeID := tv.Any().String()
	engineContext := chasm.NewEngineContext(ctx, s.chasmEngine)
	createResp, err := tests.NewPayloadStoreHandler(
		engineContext,
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	archetypeID, ok := s.FunctionalTestBase.GetTestCluster().Host().GetCHASMRegistry().ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)

	visQuery := fmt.Sprintf("TemporalNamespaceDivision = '%d' AND WorkflowId = '%s'", archetypeID, storeID)

	var visRecord *chasm.ExecutionInfo[*testspb.TestPayloadStore]
	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](engineContext, &chasm.ListExecutionsRequest{
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
			// Wait for search attributes to be populated
			_, ok := chasm.GetValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalCountSearchAttribute)
			return ok
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
	totalCountSA, ok := chasm.GetValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalCountSearchAttribute)
	s.True(ok)
	s.Equal(0, int(totalCountSA))
	totalSizeSA, ok := chasm.GetValue(visRecord.ChasmSearchAttributes, tests.PayloadTotalSizeSearchAttribute)
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
		engineContext,
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
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](engineContext, &chasm.ListExecutionsRequest{
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
			return visRecord.ChasmMemo.TotalCount == addPayloadResp.State.TotalCount
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	// We validated Count memo field above, just checking for size here.
	s.Equal(addPayloadResp.State.TotalSize, visRecord.ChasmMemo.TotalSize)

	_, err = tests.ClosePayloadStoreHandler(
		engineContext,
		tests.ClosePayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	s.Eventually(
		func() bool {
			resp, err := chasm.ListExecutions[*tests.PayloadStore, *testspb.TestPayloadStore](engineContext, &chasm.ListExecutionsRequest{
				NamespaceID:   string(s.NamespaceID()),
				NamespaceName: string(s.Namespace()),
				PageSize:      10,
				Query:         visQuery + " AND ExecutionStatus = 'Completed'",
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

// TODO: More tests here...

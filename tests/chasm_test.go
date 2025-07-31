package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const (
	chasmTestTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

type ChasmTestSuite struct {
	testcore.FunctionalTestBase

	chasmEngine chasm.Engine
}

func TestChasmTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ChasmTestSuite))
}

func (s *ChasmTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
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
			NamespaceID: s.NamespaceID(),
			StoreID:     tv.Any().String(),
		},
	)
	s.NoError(err)
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

	visQuery := fmt.Sprintf("TemporalNamespaceDivision = 'tests.payloadStore' AND WorkflowId = '%s'", storeID)

	var visRecord *workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     visQuery,
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
	s.Equal(storeID, visRecord.Execution.WorkflowId)
	s.Equal(createResp.RunID, visRecord.Execution.RunId)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, visRecord.Status)
	s.NotEmpty(visRecord.StartTime)
	s.NotEmpty(visRecord.ExecutionTime)
	s.Empty(visRecord.StateTransitionCount)
	s.Empty(visRecord.CloseTime)
	s.Empty(visRecord.HistoryLength)

	var intVal int
	p, ok := visRecord.Memo.Fields[tests.TotalCountMemoFieldName]
	s.True(ok)
	s.NoError(payload.Decode(p, &intVal))
	s.Equal(0, intVal)
	p, ok = visRecord.Memo.Fields[tests.TotalSizeMemoFieldName]
	s.True(ok)
	s.NoError(payload.Decode(p, &intVal))
	s.Equal(0, intVal)
	var strVal string
	s.NoError(payload.Decode(visRecord.SearchAttributes.IndexedFields[tests.TestKeywordSAFieldName], &strVal))
	s.Equal(tests.TestKeywordSAFieldValue, strVal)

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
			resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     visQuery + " AND ExecutionStatus = 'Completed'",
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
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, visRecord.Status)
	s.Equal(int64(2), visRecord.StateTransitionCount)
	s.NotEmpty(visRecord.CloseTime)
	s.NotEmpty(visRecord.ExecutionDuration)
	s.Empty(visRecord.HistoryLength)
}

// TODO: More tests here...

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
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

// TODO: More tests here...

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

	var visRecord *workflow.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     "TemporalNamespaceDivision = 'tests.payloadStore'",
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
	s.Equal(enums.WORKFLOW_EXECUTION_STATUS_RUNNING, visRecord.Status)
	s.NotEmpty(visRecord.StartTime)
	s.NotEmpty(visRecord.ExecutionTime)
	s.Empty(visRecord.StateTransitionCount)
	s.Empty(visRecord.CloseTime)
	s.Empty(visRecord.HistoryLength)

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
				Query:     "TemporalNamespaceDivision = 'tests.payloadStore'",
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
	s.Equal(enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, visRecord.Status)
	s.Equal(int64(2), visRecord.StateTransitionCount)
	s.NotEmpty(visRecord.CloseTime)
	s.NotEmpty(visRecord.ExecutionDuration)
	s.Empty(visRecord.HistoryLength)
}

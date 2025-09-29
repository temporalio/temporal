package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAliasSearchAttributeTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowAliasSearchAttributeTestSuite(t *testing.T) {
	s := new(WorkflowAliasSearchAttributeTestSuite)
	suite.Run(t, s)
}

func (ts *WorkflowAliasSearchAttributeTestSuite) SetupTest() {
	ts.FunctionalTestBase.SetupTest()

	ts.Worker().RegisterWorkflow(ts.WorkflowFunc)
}

func (ts *WorkflowAliasSearchAttributeTestSuite) WorkflowFunc(ctx workflow.Context) error {
	return nil
}

func (ts *WorkflowAliasSearchAttributeTestSuite) createWorkflow(
	ctx context.Context,
	workflowFn WorkflowFunction,
	prefix, customSA string) (sdkclient.WorkflowRun, error) {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(prefix + "-" + ts.T().Name()),
		TaskQueue: ts.TaskQueue(),
	}
	if customSA != "" {
		customSAKey := temporal.NewSearchAttributeKeyKeyword("CustomSA")
		workflowOptions.TypedSearchAttributes = temporal.NewSearchAttributes(
			customSAKey.ValueSet(customSA),
		)
	}
	return ts.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.SdkClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomSA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.NoError(s.T(), err)

	workflowRun1, err := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_1", "CustomValue1")
	require.NoError(s.T(), err)
	workflowRun2, err := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_2", "CustomValue2")
	require.NoError(s.T(), err)

	// Wait for workflows to complete
	require.NoError(s.T(), workflowRun1.Get(ctx, nil))
	require.NoError(s.T(), workflowRun2.Get(ctx, nil))

	searchAttributePairs := []struct {
		sa1       string
		sa2       string
		predicate string
	}{
		{sa1: "CustomSA", sa2: "CustomSA", predicate: "IS NOT NULL"},
		{sa1: "ExecutionStatus", sa2: "ExecutionStatus", predicate: "IS NOT NULL"},
		{sa1: "ScheduledStartTime", sa2: "TemporalScheduledStartTime", predicate: "IS NULL"},
		{sa1: "SchedulePaused", sa2: "TemporalSchedulePaused", predicate: "IS NULL"},
		{sa1: "PauseInfo", sa2: "TemporalPauseInfo", predicate: "IS NULL"},
		{sa1: searchattribute.ScheduleID, sa2: searchattribute.WorkflowID, predicate: "IS NOT NULL"},
	}

	for _, pair := range searchAttributePairs {
		s.EventuallyWithT(
			func(t *assert.CollectT) {
				resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("%s %s", pair.sa1, pair.predicate),
				})
				require.NoError(t, err, fmt.Sprintf("expected %s to return no error", pair.sa1))
				require.NotNil(t, resp, fmt.Sprintf("expected %s to return results", pair.sa1))
				require.Equal(t, len(resp.GetExecutions()), 2, fmt.Sprintf("expected %s to return 2 results", pair.sa1))

				resp2, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("%s %s", pair.sa2, pair.predicate),
				})
				require.NoError(t, err, fmt.Sprintf("expected %s to return no error", pair.sa2))
				require.NotNil(t, resp2, fmt.Sprintf("expected %s to return results", pair.sa2))
				require.Equal(t, len(resp2.GetExecutions()), 2, fmt.Sprintf("expected %s to return 2 results", pair.sa2))

				require.Equal(t, resp.GetExecutions(), resp2.GetExecutions(), fmt.Sprintf("expected %s and %s to return same results", pair.sa1, pair.sa2))
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

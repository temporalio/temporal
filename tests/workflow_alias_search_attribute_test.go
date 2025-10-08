package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAliasSearchAttributeTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowAliasSearchAttributeTestSuite(t *testing.T) {
	s := new(WorkflowAliasSearchAttributeTestSuite)
	suite.Run(t, s)
}

func (s *WorkflowAliasSearchAttributeTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.Worker().RegisterWorkflow(s.workflowFunc)
}

func (s *WorkflowAliasSearchAttributeTestSuite) workflowFunc(ctx workflow.Context) (string, error) {
	return "done!", nil
}

// func (s *WorkflowAliasSearchAttributeTestSuite) createWorkflow(
// 	ctx context.Context,
// 	prefix string,
// 	tv *testvars.TestVars,
// ) (*workflowservice.StartWorkflowExecutionResponse, error) {
// 	request := &workflowservice.StartWorkflowExecutionRequest{
// 		RequestId:          tv.Any().String(),
// 		Namespace:          s.Namespace().String(),
// 		WorkflowId:         tv.WorkflowID() + "_" + prefix,
// 		WorkflowType:       tv.WorkflowType(),
// 		TaskQueue:          tv.TaskQueue(),
// 		Identity:           tv.WorkerIdentity(),
// 		VersioningOverride: tv.VersioningOverridePinned(true),
// 	}
// 	return s.FrontendClient().StartWorkflowExecution(ctx, request)
// }

func (s *WorkflowAliasSearchAttributeTestSuite) createWorkflow(
	ctx context.Context,
	prefix string,
	tv *testvars.TestVars,
	sa *commonpb.SearchAttributes,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID() + "_" + prefix,
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: tv.VersioningOverridePinned(true),
		SearchAttributes:   sa,
	}
	return s.FrontendClient().StartWorkflowExecution(ctx, request)
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s.T())

	_, err := s.createWorkflow(ctx, "wf_id_1", tv, nil)
	require.NoError(s.T(), err)
	_, err = s.createWorkflow(ctx, "wf_id_2", tv, nil)
	require.NoError(s.T(), err)

	searchAttributePairs := []struct {
		sa1       string
		sa2       string
		predicate string
	}{
		{sa1: searchattribute.BuildIds, sa2: "TemporalBuildIds", predicate: "IS NOT NULL"},
		{sa1: searchattribute.TemporalWorkflowVersioningBehavior, sa2: "WorkflowVersioningBehavior", predicate: "IS NOT NULL"},
		{sa1: searchattribute.TemporalWorkerDeploymentVersion, sa2: "WorkerDeploymentVersion", predicate: "IS NOT NULL"},
		{sa1: searchattribute.TemporalWorkerDeployment, sa2: "WorkerDeployment", predicate: "IS NOT NULL"},
		{sa1: searchattribute.ScheduleID, sa2: searchattribute.WorkflowID, predicate: "IS NOT NULL"},
	}

	for _, pair := range searchAttributePairs {
		s.EventuallyWithT(
			func(t *assert.CollectT) {
				resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
				})
				require.NoError(t, err, fmt.Sprintf("expected %s to return no error", pair.sa1))
				require.NotNil(t, resp, fmt.Sprintf("expected %s to return results", pair.sa1))
				require.Equal(t, len(resp.GetExecutions()), 2, fmt.Sprintf("expected %s to return 2 results", pair.sa1))

				queriedResp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("%s %s", pair.sa1, pair.predicate),
				})
				require.NoError(t, err, fmt.Sprintf("expected %s to return no error", pair.sa1))
				require.NotNil(t, queriedResp, fmt.Sprintf("expected %s to return results", pair.sa1))
				require.Equal(t, len(queriedResp.GetExecutions()), 2, fmt.Sprintf("expected %s to return 2 results", pair.sa1))

				queriedResp2, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
					Namespace: s.Namespace().String(),
					Query:     fmt.Sprintf("%s %s", pair.sa2, pair.predicate),
				})
				require.NoError(t, err, fmt.Sprintf("expected %s to return no error", pair.sa2))
				require.NotNil(t, queriedResp2, fmt.Sprintf("expected %s to return results", pair.sa2))
				require.Equal(t, len(queriedResp2.GetExecutions()), 2, fmt.Sprintf("expected %s to return 2 results", pair.sa2))

				require.Equal(t, queriedResp.GetExecutions(), resp.GetExecutions(), fmt.Sprintf("expected %s to return all wf executions", pair.sa1))
				require.Equal(t, queriedResp.GetExecutions(), queriedResp2.GetExecutions(), fmt.Sprintf("expected %s and %s to return same results", pair.sa1, pair.sa2))

			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute_CustomSearchAttributeOverride() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s.T())

	_, err := s.SdkClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"WorkflowVersioningBehavior": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	require.NoError(s.T(), err)

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"WorkflowVersioningBehavior": payload.EncodeString("user-defined"),
		},
	}

	_, err = s.createWorkflow(ctx, "wf_id_1", tv, sa)
	require.NoError(s.T(), err)
	_, err = s.createWorkflow(ctx, "wf_id_2", tv, sa)
	require.NoError(s.T(), err)

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, len(resp.GetExecutions()), 2)

			queriedResp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("%s = 'Pinned'", searchattribute.TemporalWorkflowVersioningBehavior),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, len(queriedResp.GetExecutions()), 2)

			queriedResp, err = s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowVersioningBehavior = 'user-defined'",
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, len(queriedResp.GetExecutions()), 2)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

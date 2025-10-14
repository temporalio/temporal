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

func (s *WorkflowAliasSearchAttributeTestSuite) createWorkflow(
	ctx context.Context,
	tv *testvars.TestVars,
	sa *commonpb.SearchAttributes,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: tv.VersioningOverridePinned(true),
		SearchAttributes:   sa,
	}
	return s.FrontendClient().StartWorkflowExecution(ctx, request)
}

func (s *WorkflowAliasSearchAttributeTestSuite) terminateWorkflow(
	ctx context.Context,
	tv *testvars.TestVars,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
	return s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
		},
		Reason: "terminate reason",
	})
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s.T())

	_, err := s.createWorkflow(ctx, tv, nil)
	s.Require().NoError(err)

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.GetExecutions(), 1)

			queriedResp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("%s = 'Pinned'", searchattribute.TemporalWorkflowVersioningBehavior),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, queriedResp.GetExecutions(), 1)

			queriedResp, err = s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowVersioningBehavior = 'Pinned'",
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, queriedResp.GetExecutions(), 1)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	_, err = s.terminateWorkflow(ctx, tv)
	s.Require().NoError(err)
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
	s.Require().NoError(err)

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"WorkflowVersioningBehavior": payload.EncodeString("user-defined"),
		},
	}

	_, err = s.createWorkflow(ctx, tv, sa)
	s.Require().NoError(err)

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			queriedResp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("%s = 'Pinned'", searchattribute.TemporalWorkflowVersioningBehavior),
			})
			require.NoError(t, err)
			require.NotNil(t, queriedResp)
			require.Len(t, queriedResp.GetExecutions(), 1)

			queriedResp, err = s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowVersioningBehavior = 'user-defined'",
			})
			require.NoError(t, err)
			require.NotNil(t, queriedResp)
			require.Len(t, queriedResp.GetExecutions(), 1)
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	_, err = s.terminateWorkflow(ctx, tv)
	s.Require().NoError(err)
}

package tdbg_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc"
)

type visibilityAdminClient struct {
	adminservice.AdminServiceClient

	listPages    []*adminservice.ListExecutionsResponse
	listNext     int
	listRequests []*adminservice.ListExecutionsRequest
	listErr      error

	countResponse *adminservice.CountExecutionsResponse
	countRequests []*adminservice.CountExecutionsRequest
	countErr      error
}

func (c *visibilityAdminClient) ListExecutions(
	_ context.Context,
	req *adminservice.ListExecutionsRequest,
	_ ...grpc.CallOption,
) (*adminservice.ListExecutionsResponse, error) {
	// The command reuses one request struct across pages, so record a copy.
	c.listRequests = append(c.listRequests, common.CloneProto(req))
	if c.listErr != nil {
		return nil, c.listErr
	}
	resp := c.listPages[c.listNext]
	c.listNext++
	return resp, nil
}

func (c *visibilityAdminClient) CountExecutions(
	_ context.Context,
	req *adminservice.CountExecutionsRequest,
	_ ...grpc.CallOption,
) (*adminservice.CountExecutionsResponse, error) {
	c.countRequests = append(c.countRequests, req)
	if c.countErr != nil {
		return nil, c.countErr
	}
	return c.countResponse, nil
}

func runVisibility(
	t *testing.T,
	admin adminservice.AdminServiceClient,
	args ...string,
) (stdoutStr, stderrStr string, err error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	app := tdbgtest.NewCliApp(func(params *tdbg.Params) {
		params.ClientFactory = migrateClientFactory{admin: admin}
		params.Writer = &stdout
		params.ErrWriter = &stderr
	})
	err = app.Run(append([]string{"tdbg"}, args...))
	return stdout.String(), stderr.String(), err
}

func visibilityExecution(workflowID string) *persistencespb.VisibilityExecutionInfo {
	return &persistencespb.VisibilityExecutionInfo{
		NamespaceId: "test-namespace-id",
		Namespace:   "test-namespace",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      "test-run-id",
		},
		WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
		Status:       enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}
}

func TestAdminListExecutions(t *testing.T) {
	admin := &visibilityAdminClient{
		listPages: []*adminservice.ListExecutionsResponse{
			{
				Executions: []*persistencespb.VisibilityExecutionInfo{
					visibilityExecution("wid-1"),
				},
			},
		},
	}

	stdout, _, err := runVisibility(
		t, admin,
		"-n", "my-ns",
		"visibility", "list",
		"--query", "ExecutionStatus = 'Completed'",
		"--print-json",
	)
	require.NoError(t, err)

	require.Len(t, admin.listRequests, 1)
	require.Equal(t, "my-ns", admin.listRequests[0].Namespace)
	require.Equal(t, "ExecutionStatus = 'Completed'", admin.listRequests[0].Query)
	require.Equal(t, int32(10), admin.listRequests[0].PageSize) // flag default
	require.Empty(t, admin.listRequests[0].NextPageToken)
	require.Contains(t, stdout, "wid-1")
	require.Contains(t, stdout, "test-namespace")
}

// TestAdminListExecutions_AllNamespaces covers omitting --namespace, which must send an
// empty namespace so the server scans every namespace. The global --namespace flag defaults
// to "default", so the command has to distinguish "unset" from "explicitly default".
func TestAdminListExecutions_AllNamespaces(t *testing.T) {
	admin := &visibilityAdminClient{
		listPages: []*adminservice.ListExecutionsResponse{
			{
				Executions: []*persistencespb.VisibilityExecutionInfo{
					visibilityExecution("wid-1"),
				},
			},
		},
	}

	_, _, err := runVisibility(t, admin, "visibility", "list", "--print-json")
	require.NoError(t, err)

	require.Len(t, admin.listRequests, 1)
	require.Empty(t, admin.listRequests[0].Namespace)
}

func TestAdminListExecutions_Paginates(t *testing.T) {
	admin := &visibilityAdminClient{
		listPages: []*adminservice.ListExecutionsResponse{
			{
				Executions: []*persistencespb.VisibilityExecutionInfo{
					visibilityExecution("wid-1"),
				},
				NextPageToken: []byte("page-2"),
			},
			{
				Executions: []*persistencespb.VisibilityExecutionInfo{
					visibilityExecution("wid-2"),
				},
			},
		},
	}

	// A page size of 2 spans both pages, so the iterator fetches them both before printing.
	stdout, _, err := runVisibility(
		t, admin,
		"visibility", "list",
		"--pagesize", "2",
		"--print-json",
	)
	require.NoError(t, err)

	require.Len(t, admin.listRequests, 2)
	require.Empty(t, admin.listRequests[0].NextPageToken)
	require.Equal(t, []byte("page-2"), admin.listRequests[1].NextPageToken)
	require.Contains(t, stdout, "wid-1")
	require.Contains(t, stdout, "wid-2")
}

func TestAdminListExecutions_Error(t *testing.T) {
	admin := &visibilityAdminClient{listErr: errors.New("list failed")}

	_, _, err := runVisibility(t, admin, "visibility", "list")
	require.ErrorContains(t, err, "unable to list executions")
	require.ErrorContains(t, err, "list failed")
}

func TestAdminCountExecutions(t *testing.T) {
	admin := &visibilityAdminClient{
		countResponse: &adminservice.CountExecutionsResponse{Count: 42},
	}

	stdout, _, err := runVisibility(
		t, admin,
		"-n", "my-ns",
		"visibility", "count",
		"--query", "ExecutionStatus = 'Completed'",
	)
	require.NoError(t, err)

	require.Len(t, admin.countRequests, 1)
	require.Equal(t, "my-ns", admin.countRequests[0].Namespace)
	require.Equal(t, "ExecutionStatus = 'Completed'", admin.countRequests[0].Query)
	require.Contains(t, stdout, "42")
}

// TestAdminCountExecutions_AllNamespaces mirrors TestAdminListExecutions_AllNamespaces:
// omitting --namespace must send an empty namespace rather than the flag's "default".
func TestAdminCountExecutions_AllNamespaces(t *testing.T) {
	admin := &visibilityAdminClient{
		countResponse: &adminservice.CountExecutionsResponse{Count: 42},
	}

	_, _, err := runVisibility(t, admin, "visibility", "count")
	require.NoError(t, err)

	require.Len(t, admin.countRequests, 1)
	require.Empty(t, admin.countRequests[0].Namespace)
}

func TestAdminCountExecutions_Error(t *testing.T) {
	admin := &visibilityAdminClient{countErr: errors.New("count failed")}

	_, _, err := runVisibility(t, admin, "visibility", "count")
	require.ErrorContains(t, err, "unable to count executions")
	require.ErrorContains(t, err, "count failed")
}

// TestAdminVisibilityAlias covers the "vis" alias for the visibility command group.
func TestAdminVisibilityAlias(t *testing.T) {
	admin := &visibilityAdminClient{
		countResponse: &adminservice.CountExecutionsResponse{Count: 1},
	}
	_, _, err := runVisibility(t, admin, "vis", "count")
	require.NoError(t, err)
	require.Len(t, admin.countRequests, 1)
}

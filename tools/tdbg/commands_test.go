package tdbg

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
)

type listClusterMembersClient struct {
	adminservice.AdminServiceClient
	requests []*adminservice.ListClusterMembersRequest
}

func (c *listClusterMembersClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return c
}

func (c *listClusterMembersClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	panic("unimplemented")
}

func (c *listClusterMembersClient) ListClusterMembers(
	_ context.Context,
	request *adminservice.ListClusterMembersRequest,
	_ ...grpc.CallOption,
) (*adminservice.ListClusterMembersResponse, error) {
	c.requests = append(c.requests, request)
	return &adminservice.ListClusterMembersResponse{}, nil
}

func TestAdminListClusterMembersHeartbeatedWithin(t *testing.T) {
	testCases := []struct {
		name     string
		args     []string
		expected time.Duration
	}{
		{
			name:     "default",
			expected: 15 * time.Minute,
		},
		{
			name:     "time range",
			args:     []string{"--heartbeated-within", "2h"},
			expected: 2 * time.Hour,
		},
		{
			name:     "empty disables the filter",
			args:     []string{"--heartbeated-within", ""},
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &listClusterMembersClient{}
			app := NewCliApp(func(params *Params) {
				params.ClientFactory = client
				params.Writer = io.Discard
			})

			err := app.Run(append([]string{"tdbg", "membership", "list-db"}, tc.args...))
			require.NoError(t, err)
			require.Len(t, client.requests, 1)
			require.Equal(t, tc.expected, client.requests[0].GetLastHeartbeatWithin().AsDuration())
		})
	}
}

func TestGetCategory(t *testing.T) {
	cat101 := tasks.NewCategory(101, tasks.CategoryTypeImmediate, "CategoryName")

	registry := tasks.NewDefaultTaskCategoryRegistry()
	registry.AddCategory(cat101)

	testCases := []struct {
		name        string
		input       string
		expectedCat tasks.Category
		expectedErr string
	}{
		{
			name:        "same case",
			input:       "CategoryName",
			expectedCat: cat101,
		},
		{
			name:        "different case",
			input:       "cAtEgOrYnAmE",
			expectedCat: cat101,
		},
		{
			name:        "not found",
			input:       "random",
			expectedErr: "unknown task category \"random\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := require.New(t)
			cat, err := getCategory(registry, tc.input)
			if tc.expectedErr == "" {
				s.NoError(err)
				s.Equal(tc.expectedCat, cat)
			} else {
				s.ErrorContains(err, tc.expectedErr)
			}
		})
	}
}

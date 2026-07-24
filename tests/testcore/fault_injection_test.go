//go:build test_dep

package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestRPCFaultOptionsMatchesNamespace(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		options rpcFaultOptions
		request any
		matches bool
	}{
		{
			name:    "no filters",
			request: struct{}{},
			matches: true,
		},
		{
			name:    "matching namespace ID",
			options: rpcFaultOptions{namespaceID: "namespace-id"},
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			matches: true,
		},
		{
			name: "matching namespace ID with both filters",
			options: rpcFaultOptions{
				namespaceID:   "namespace-id",
				namespaceName: "namespace-name",
			},
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
			matches: true,
		},
		{
			name:    "mismatched namespace ID",
			options: rpcFaultOptions{namespaceID: "namespace-id"},
			request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "other-namespace-id"},
		},
		{
			name:    "empty namespace ID",
			options: rpcFaultOptions{namespaceID: "namespace-id"},
			request: &matchingservice.AddWorkflowTaskRequest{},
		},
		{
			name:    "matching namespace name",
			options: rpcFaultOptions{namespaceName: "namespace-name"},
			request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"},
			matches: true,
		},
		{
			name: "matching namespace name with both filters",
			options: rpcFaultOptions{
				namespaceID:   "namespace-id",
				namespaceName: "namespace-name",
			},
			request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"},
			matches: true,
		},
		{
			name:    "mismatched namespace name",
			options: rpcFaultOptions{namespaceName: "namespace-name"},
			request: &workflowservice.StartWorkflowExecutionRequest{Namespace: "other-namespace-name"},
		},
		{
			name: "namespace-less request",
			options: rpcFaultOptions{
				namespaceID:   "namespace-id",
				namespaceName: "namespace-name",
			},
			request: struct{}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.matches, tc.options.matchesNamespace(tc.request))
		})
	}
}

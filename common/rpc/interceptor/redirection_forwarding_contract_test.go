package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestRedirectionForwardingContract(t *testing.T) {
	const (
		namespaceName = "forwarding-contract"
		localRunID    = "local-run-id"
		remoteRunID   = "remote-run-id"
		fullMethod    = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
	)
	localCluster := cluster.TestCurrentClusterName
	remoteCluster := cluster.TestAlternativeClusterName
	localInactiveErr := serviceerror.NewNamespaceNotActive(namespaceName, localCluster, remoteCluster)
	lookupErr := serviceerror.NewUnavailable("remote client lookup failed")
	invokeErr := serviceerror.NewUnavailable("remote invocation failed")
	localOtherErr := serviceerror.NewInvalidArgument("local request invalid")

	for _, policy := range []string{DCRedirectionPolicySelectedAPIsForwarding, DCRedirectionPolicyAllAPIsForwarding} {
		t.Run(policy, func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				cachedActive    string
				localErr        error
				remoteLookupErr error
				remoteInvokeErr error
				requestDisabled bool
				configDisabled  bool
				wantErr         error
				wantRunID       string
				wantResponse    bool
				wantCalls       []string
			}{
				{
					name: "local_then_remote_success", cachedActive: localCluster, localErr: localInactiveErr,
					wantRunID: remoteRunID, wantResponse: true,
					wantCalls: []string{"local", "remote-client", "remote-invoke"},
				},
				{
					name: "local_success", cachedActive: localCluster,
					wantRunID: localRunID, wantResponse: true, wantCalls: []string{"local"},
				},
				{
					name: "direct_remote_success", cachedActive: remoteCluster,
					wantRunID: remoteRunID, wantResponse: true, wantCalls: []string{"remote-client", "remote-invoke"},
				},
				{
					name: "remote_lookup_failure", cachedActive: localCluster, localErr: localInactiveErr,
					remoteLookupErr: lookupErr, wantErr: lookupErr, wantCalls: []string{"local", "remote-client"},
				},
				{
					name: "remote_invocation_failure", cachedActive: localCluster, localErr: localInactiveErr,
					remoteInvokeErr: invokeErr, wantErr: invokeErr, wantResponse: true,
					wantCalls: []string{"local", "remote-client", "remote-invoke"},
				},
				{
					name: "request_redirection_disabled", cachedActive: remoteCluster, localErr: localInactiveErr,
					requestDisabled: true, wantErr: localInactiveErr, wantCalls: []string{"local"},
				},
				{
					name: "config_redirection_disabled", cachedActive: remoteCluster, localErr: localInactiveErr,
					configDisabled: true, wantErr: localInactiveErr, wantCalls: []string{"local"},
				},
				{
					name: "local_nonretryable_failure", cachedActive: localCluster, localErr: localOtherErr,
					wantErr: localOtherErr, wantCalls: []string{"local"},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					controller := gomock.NewController(t)
					registry := namespace.NewMockRegistry(controller)
					bean := client.NewMockBean(controller)
					clusterMetadata := cluster.NewMockMetadata(controller)
					clusterMetadata.EXPECT().GetCurrentClusterName().Return(localCluster).Times(2)
					entry := namespace.NewGlobalNamespaceForTest(
						&persistencespb.NamespaceInfo{Id: "deadd0d0-c001-face-d00d-000000000000", Name: namespaceName},
						&persistencespb.NamespaceConfig{},
						&persistencespb.NamespaceReplicationConfig{
							ActiveClusterName: tc.cachedActive,
							Clusters:          []string{localCluster, remoteCluster},
						},
						1,
					)
					if !tc.requestDisabled {
						registry.EXPECT().GetNamespace(namespace.Name(namespaceName)).Return(entry, nil).Times(2)
					}
					redirector := NewRedirection(
						dynamicconfig.GetBoolPropertyFnFilteredByNamespace(!tc.configDisabled),
						dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
						registry, config.DCRedirectionPolicy{Policy: policy}, log.NewNoopLogger(),
						bean, metrics.NoopMetricsHandler, clock.NewRealTimeSource(), clusterMetadata,
					)
					request := &workflowservice.StartWorkflowExecutionRequest{
						Namespace: namespaceName, WorkflowId: "forwarding-contract-workflow", RequestId: "forwarding-contract-request",
					}
					var calls []string
					remoteInvocations := 0
					remoteConn := &redirectionForwardingContractConn{
						invoke: func(ctx context.Context, method string, req, reply any, _ ...grpc.CallOption) error {
							calls = append(calls, "remote-invoke")
							remoteInvocations++
							require.Equal(t, fullMethod, method)
							require.Same(t, request, req)
							md, ok := metadata.FromOutgoingContext(ctx)
							require.True(t, ok)
							require.Equal(t, []string{"true"}, md.Get(DCRedirectionAPIHeaderName))
							require.Equal(t, []string{localCluster}, md.Get(DCRedirectionSourceCellHeaderName))
							require.IsType(t, &workflowservice.StartWorkflowExecutionResponse{}, reply)
							if tc.remoteInvokeErr == nil {
								reply.(*workflowservice.StartWorkflowExecutionResponse).RunId = remoteRunID
							}
							return tc.remoteInvokeErr
						},
					}
					if len(tc.wantCalls) > 1 {
						bean.EXPECT().GetRemoteFrontendClient(remoteCluster).DoAndReturn(
							func(string) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error) {
								calls = append(calls, "remote-client")
								return remoteConn, nil, tc.remoteLookupErr
							},
						).Times(1)
					}
					ctx := t.Context()
					if tc.requestDisabled {
						ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(DCRedirectionContextHeaderName, "false"))
					}
					response, err := redirector.Intercept(ctx, request, &grpc.UnaryServerInfo{FullMethod: fullMethod},
						func(_ context.Context, req any) (any, error) {
							calls = append(calls, "local")
							require.Same(t, request, req)
							if tc.localErr != nil {
								return nil, tc.localErr
							}
							return &workflowservice.StartWorkflowExecutionResponse{RunId: localRunID}, nil
						})
					t.Logf("calls=%v remote_invocations=%d response=%v returned_error=%T(%v) same_as_local_error=%t",
						calls, remoteInvocations, response, err, err, err != nil && err == tc.localErr)
					require.Equal(t, tc.wantCalls, calls)
					wantRemoteInvocations := 0
					if tc.wantResponse && tc.wantRunID != localRunID {
						wantRemoteInvocations = 1
					}
					require.Equal(t, wantRemoteInvocations, remoteInvocations)
					if tc.wantResponse {
						require.IsType(t, &workflowservice.StartWorkflowExecutionResponse{}, response)
						require.Equal(t, tc.wantRunID, response.(*workflowservice.StartWorkflowExecutionResponse).GetRunId())
					} else {
						require.Nil(t, response)
					}
					if tc.wantErr != nil {
						require.ErrorIs(t, err, tc.wantErr)
					} else {
						require.NoError(t, err, "the final successful attempt must determine the RPC result")
					}
				})
			}
		})
	}
}

type redirectionForwardingContractConn struct {
	grpc.ClientConnInterface
	invoke func(context.Context, string, any, any, ...grpc.CallOption) error
}

func (c *redirectionForwardingContractConn) Invoke(ctx context.Context, method string, req, reply any, opts ...grpc.CallOption) error {
	return c.invoke(ctx, method, req, reply, opts...)
}

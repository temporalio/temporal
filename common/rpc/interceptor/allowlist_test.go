package interceptor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

// Bare method names are crowded even inside this server: ListTaskQueuePartitions is on
// both WorkflowService and MatchingService, and the three replication reads below are on
// both AdminService and HistoryService. Only the first of each pair reaches the frontend
// chain, so keying by bare name was harmless there — but an embedder registers its
// services on that same chain, where the collision does land.
func TestHandoverAllowed_ServerListDoesNotReachOtherServices(t *testing.T) {
	none := map[string]struct{}{}

	require.Contains(t, allowedMethodsDuringHandover, api.WorkflowServicePrefix+"ListTaskQueuePartitions")
	require.True(t, handoverAllowed(api.WorkflowServicePrefix+"ListTaskQueuePartitions", none))
	require.False(t, handoverAllowed(api.MatchingServicePrefix+"ListTaskQueuePartitions", none),
		"MatchingService has the same method name and must not inherit the entry")

	require.True(t, handoverAllowed(api.AdminServicePrefix+"GetReplicationMessages", none))
	require.False(t, handoverAllowed(api.HistoryServicePrefix+"GetReplicationMessages", none),
		"HistoryService has the same method name and must not inherit the entry")
}

// GetSearchAttributes exists on both WorkflowService and AdminService, and the frontend
// serves both. Re-keying turned one bare entry into two; dropping either would silently
// start gating that service's call during a handover.
func TestHandoverAllowed_MultiServiceMethodCoversEveryService(t *testing.T) {
	none := map[string]struct{}{}
	require.True(t, handoverAllowed(api.WorkflowServicePrefix+"GetSearchAttributes", none))
	require.True(t, handoverAllowed(api.AdminServicePrefix+"GetSearchAttributes", none))
}

func TestHandoverAllowed_EmbedderEntriesAreFullMethodScoped(t *testing.T) {
	// MatchingService stands in for a service an embedder registers: real, with a
	// descriptor, and sharing a method name with a WorkflowService entry.
	additional := newAdditionalAllowedMethods([]string{api.MatchingServicePrefix + "DescribeTaskQueue"})

	require.True(t, handoverAllowed(api.MatchingServicePrefix+"DescribeTaskQueue", additional))
	require.False(t, handoverAllowed(api.WorkflowServicePrefix+"DescribeTaskQueue", additional),
		"an embedder entry must not allow the server's same-named method")
	require.False(t, handoverAllowed(api.MatchingServicePrefix+"ListWorkers", additional))
}

func TestSelectedAPIsForwarding_AllowlistIsFullMethodScoped(t *testing.T) {
	require.Contains(t, selectedAPIsForwardingRedirectionPolicyAllowedAPIs,
		api.WorkflowServicePrefix+"SignalWorkflowExecution")

	policy := &SelectedAPIsForwardingRedirectionPolicy{}
	require.True(t, policy.allowed(api.WorkflowServicePrefix+"SignalWorkflowExecution"))
	require.False(t, policy.allowed(api.HistoryServicePrefix+"SignalWorkflowExecution"),
		"HistoryService has the same method name and must not inherit the entry")

	// ScheduleWorkflowTask exists on HistoryService and is deliberately absent from the
	// server's allow-list: no client calls it. An embedder that needs it forwarded between
	// clusters uses the extension point.
	require.NotContains(t, selectedAPIsForwardingRedirectionPolicyAllowedAPIs,
		api.HistoryServicePrefix+"ScheduleWorkflowTask")
	require.False(t, policy.allowed(api.HistoryServicePrefix+"ScheduleWorkflowTask"))

	extended := policy.WithAdditionalAllowedMethods(api.HistoryServicePrefix + "ScheduleWorkflowTask")
	require.True(t, extended.allowed(api.HistoryServicePrefix+"ScheduleWorkflowTask"))
	require.False(t, extended.allowed(api.WorkflowServicePrefix+"ScheduleWorkflowTask"),
		"the addition must not widen the server's own surface")
	require.False(t, policy.allowed(api.HistoryServicePrefix+"ScheduleWorkflowTask"),
		"WithAdditionalAllowedMethods must not mutate the receiver")
}

// This server's own two lists, asserted the way an embedder should assert theirs: nothing
// checks them at runtime, because one inert entry is a smaller problem than a frontend
// that will not start. A bare entry would match nothing, which fails open for the
// handover list and closed for the forwarding one, and neither failure announces itself —
// so the assertion lives here.
func TestServerListsAreValidFullMethods(t *testing.T) {
	for _, tc := range []struct {
		name string
		list map[string]struct{}
	}{
		{"allowedMethodsDuringHandover", allowedMethodsDuringHandover},
		{"selectedAPIsForwardingRedirectionPolicyAllowedAPIs", selectedAPIsForwardingRedirectionPolicyAllowedAPIs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := make([]string, 0, len(tc.list))
			for method := range tc.list {
				entries = append(entries, method)
			}
			require.NoError(t, validateFullMethods(entries...))
		})
	}
}

func TestValidateFullMethods(t *testing.T) {
	t.Run("accepts a real method", func(t *testing.T) {
		require.NoError(t, validateFullMethods(api.WorkflowServicePrefix+"StartWorkflowExecution"))
	})

	t.Run("rejects malformed shapes", func(t *testing.T) {
		for _, bad := range []string{
			"StartWorkflowExecution",  // bare
			"/",                       // the old check accepted this
			"/StartWorkflowExecution", // no service
			"/temporal.api.workflowservice.v1.WorkflowService/", // no method
			"/NoDotInService/Method",                            // not package-qualified
			"/a.b.C/Method/Extra",                               // too many segments
			"",
		} {
			require.ErrorContains(t, validateFullMethods(bad),
				"is not a full gRPC method", "should have rejected %q", bad)
		}
	})

	// The failure mode that happens: the service half comes from a constant while the
	// method half is typed.
	t.Run("rejects a method the service does not have", func(t *testing.T) {
		require.ErrorContains(t,
			validateFullMethods(api.AdminServicePrefix+"GetReplicationMessage"),
			`has no method "GetReplicationMessage"`)
	})

	// Nexus uses this format without being a gRPC service, and this server relies on that
	// in allowedNamespaceStatesPerAPI.
	t.Run("skips a service with no descriptor", func(t *testing.T) {
		require.NoError(t, validateFullMethods(api.NexusServicePrefix+"DispatchByEndpoint"))
	})
}

func TestParseFullMethod(t *testing.T) {
	service, method, ok := api.ParseFullMethod(api.WorkflowServicePrefix + "StartWorkflowExecution")
	require.True(t, ok)
	require.Equal(t, "temporal.api.workflowservice.v1.WorkflowService", service)
	require.Equal(t, "StartWorkflowExecution", method)

	for _, bad := range []string{"", "/", "Bare", "/svc/", "//Method", "/NoDot/Method", "/a.b.C/M/N"} {
		_, _, ok := api.ParseFullMethod(bad)
		require.False(t, ok, "should have rejected %q", bad)
	}
}

// Guards the claim the shape assertions rest on: every server-list key parses.
func TestServerListKeysParse(t *testing.T) {
	for method := range allowedMethodsDuringHandover {
		require.True(t, strings.HasPrefix(method, "/"), method)
		_, _, ok := api.ParseFullMethod(method)
		require.True(t, ok, method)
	}
}

// A bad entry is logged, not fatal: the interceptor is still usable and every good entry
// still works.
func TestNewNamespaceHandoverInterceptor_LogsInvalidEntriesAndKeepsGoing(t *testing.T) {
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Warn("handover allow-list entries will never match", gomock.Any()).Times(1)

	i := NewNamespaceHandoverInterceptor(
		dynamicconfig.NewNoopCollection(),
		nil, metrics.NoopMetricsHandler, logger, clock.NewRealTimeSource(), nil,
		[]string{"BareName", api.MatchingServicePrefix + "DescribeTaskQueue"},
	)

	require.True(t, handoverAllowed(api.MatchingServicePrefix+"DescribeTaskQueue",
		i.additionalAllowedMethodsDuringHandover))
}

// The extension has to reach the policy NewRedirection builds for itself, not just a
// policy a caller happens to hold: the field is private and there is no other way in, so
// without this an embedder could register a redirect response and still never forward.
func TestRedirection_WithAdditionalAllowedMethods(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	newRedirection := func() *Redirection {
		return NewRedirection(
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			namespace.NewMockRegistry(controller),
			config.DCRedirectionPolicy{Policy: DCRedirectionPolicySelectedAPIsForwarding},
			log.NewNoopLogger(), nil, metrics.NoopMetricsHandler, clock.NewRealTimeSource(),
			clusterMetadata,
		)
	}

	const embedderMethod = "/embedder.api.v1.Service/ScheduleWorkflowTask"

	base := newRedirection()
	require.False(t, base.redirectionPolicy.(*SelectedAPIsForwardingRedirectionPolicy).allowed(embedderMethod))

	extended := newRedirection().WithAdditionalAllowedMethods(embedderMethod)
	require.True(t, extended.redirectionPolicy.(*SelectedAPIsForwardingRedirectionPolicy).allowed(embedderMethod))

	// The server's own surface is untouched, and the receiver is not mutated.
	require.False(t, extended.redirectionPolicy.(*SelectedAPIsForwardingRedirectionPolicy).
		allowed(api.WorkflowServicePrefix+"ScheduleWorkflowTask"))
	require.False(t, base.redirectionPolicy.(*SelectedAPIsForwardingRedirectionPolicy).allowed(embedderMethod))
}

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination dc_redirection_policy_mock.go

package interceptor

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

const (
	// DCRedirectionPolicyDefault means no redirection
	DCRedirectionPolicyDefault = ""
	// DCRedirectionPolicyNoop means no redirection
	DCRedirectionPolicyNoop = "noop"
	// DCRedirectionPolicySelectedAPIsForwarding means forwarding state-effecting APIs based on namespace
	// See selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs for the list of APIs
	DCRedirectionPolicySelectedAPIsForwarding = "selected-apis-forwarding"

	// DCRedirectionPolicyAllAPIsForwarding means forwarding all APIs based on namespace active cluster
	DCRedirectionPolicyAllAPIsForwarding = "all-apis-forwarding"
)

type (
	// DCRedirectionPolicy is a DC redirection policy interface. fullMethod is the full
	// gRPC method ("/pkg.Service/Method"), not the bare name.
	DCRedirectionPolicy interface {
		WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, fullMethod string, req any, call func(string) error) error
		WithNamespaceRedirect(ctx context.Context, namespaceName namespace.Name, fullMethod string, req any, call func(string) error) error
	}

	// NoopRedirectionPolicy is DC redirection policy which does nothing
	NoopRedirectionPolicy struct {
		currentClusterName string
	}

	// SelectedAPIsForwardingRedirectionPolicy is a DC redirection policy
	// which (based on namespace) forwards selected APIs calls to active cluster
	SelectedAPIsForwardingRedirectionPolicy struct {
		currentClusterName    string
		enabledForNS          dynamicconfig.BoolPropertyFnWithNamespaceFilter
		selectedAPIsOnlyForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter
		namespaceRegistry     namespace.Registry
		selectedAPIsOnly      bool
		// additionalWhitelisted are embedder methods that forward under the
		// selected-APIs policy, keyed by full gRPC method. Nil by default. Without this
		// an embedder could register a response constructor on the Redirection
		// interceptor and still never forward, because the whitelist in
		// selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs is private
		// and holds only this server's own methods.
		additionalWhitelisted map[string]struct{}
	}
)

// selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs are the APIs the
// selected-apis-forwarding policy forwards to the active cluster, keyed by full gRPC
// method.
//
// Full methods rather than bare names because an embedder registers its own services on
// this server: a bare "DescribeTaskQueue" cannot tell WorkflowService's from another
// service's, and an entry meant for one would silently apply to the other.
var selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs = map[string]struct{}{
	// Workflow APIs
	wfMethod("StartWorkflowExecution"):           {},
	wfMethod("SignalWithStartWorkflowExecution"): {},
	wfMethod("SignalWorkflowExecution"):          {},
	wfMethod("UpdateWorkflowExecution"):          {},
	wfMethod("RequestCancelWorkflowExecution"):   {},
	wfMethod("TerminateWorkflowExecution"):       {},
	wfMethod("PauseWorkflowExecution"):           {},
	wfMethod("UnpauseWorkflowExecution"):         {},
	wfMethod("ResetWorkflowExecution"):           {},
	wfMethod("DeleteWorkflowExecution"):          {},
	wfMethod("QueryWorkflow"):                    {},
	wfMethod("ExecuteMultiOperation"):            {},

	// Standalone Activity APIs
	wfMethod("StartActivityExecution"):         {},
	wfMethod("RequestCancelActivityExecution"): {},
	wfMethod("TerminateActivityExecution"):     {},
	wfMethod("DeleteActivityExecution"):        {},
	wfMethod("PauseActivityExecution"):         {},
	wfMethod("UnpauseActivityExecution"):       {},
	wfMethod("ResetActivityExecution"):         {},
	wfMethod("UpdateActivityExecutionOptions"): {},

	// Standalone Nexus Operation APIs
	wfMethod("StartNexusOperationExecution"):         {},
	wfMethod("RequestCancelNexusOperationExecution"): {},
	wfMethod("TerminateNexusOperationExecution"):     {},
	wfMethod("DeleteNexusOperationExecution"):        {},
}

func wfMethod(name string) string { return api.WorkflowServicePrefix + name }

// RedirectionPolicyGenerator generate corresponding redirection policy
func RedirectionPolicyGenerator(
	clusterMetadata cluster.Metadata,
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	selectedAPIsOnlyForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
	policy config.DCRedirectionPolicy,
) DCRedirectionPolicy {
	switch policy.Policy {
	case DCRedirectionPolicyDefault:
		// default policy, noop
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyNoop:
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicySelectedAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewSelectedAPIsForwardingPolicy(currentClusterName, enabledForNS, selectedAPIsOnlyForNS, namespaceRegistry)
	case DCRedirectionPolicyAllAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewAllAPIsForwardingPolicy(currentClusterName, enabledForNS, selectedAPIsOnlyForNS, namespaceRegistry)
	default:
		panic(fmt.Sprintf("Unknown DC redirection policy %v", policy.Policy))
	}
}

// NewNoopRedirectionPolicy is DC redirection policy which does nothing
func NewNoopRedirectionPolicy(currentClusterName string) *NoopRedirectionPolicy {
	return &NoopRedirectionPolicy{
		currentClusterName: currentClusterName,
	}
}

// WithNamespaceIDRedirect redirect the API call based on namespace ID
func (policy *NoopRedirectionPolicy) WithNamespaceIDRedirect(_ context.Context, _ namespace.ID, _ string, _ any, call func(string) error) error {
	return call(policy.currentClusterName)
}

// WithNamespaceRedirect redirect the API call based on namespace name
func (policy *NoopRedirectionPolicy) WithNamespaceRedirect(_ context.Context, _ namespace.Name, _ string, _ any, call func(string) error) error {
	return call(policy.currentClusterName)
}

// NewSelectedAPIsForwardingPolicy creates a forwarding policy for selected APIs based on namespace
func NewSelectedAPIsForwardingPolicy(
	currentClusterName string,
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	selectedAPIsOnlyForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName:    currentClusterName,
		enabledForNS:          enabledForNS,
		selectedAPIsOnlyForNS: selectedAPIsOnlyForNS,
		namespaceRegistry:     namespaceRegistry,
		selectedAPIsOnly:      true,
	}
}

// NewAllAPIsForwardingPolicy creates a forwarding policy for all APIs based on namespace
func NewAllAPIsForwardingPolicy(
	currentClusterName string,
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	selectedAPIsOnlyForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName:    currentClusterName,
		enabledForNS:          enabledForNS,
		selectedAPIsOnlyForNS: selectedAPIsOnlyForNS,
		namespaceRegistry:     namespaceRegistry,
		selectedAPIsOnly:      false,
	}
}

// WithAdditionalWhitelistedMethods returns a copy of the policy that also forwards the
// given full gRPC methods under the selected-APIs policy.
//
// Embedders use it to opt their own methods into forwarding.
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithAdditionalWhitelistedMethods(fullMethods ...string) *SelectedAPIsForwardingRedirectionPolicy {
	clone := *policy
	clone.additionalWhitelisted = make(map[string]struct{}, len(policy.additionalWhitelisted)+len(fullMethods))
	for method := range policy.additionalWhitelisted {
		clone.additionalWhitelisted[method] = struct{}{}
	}
	for _, method := range fullMethods {
		clone.additionalWhitelisted[method] = struct{}{}
	}
	return &clone
}

// WithNamespaceIDRedirect redirect the API call based on namespace ID
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, fullMethod string, _ any, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, namespaceEntry, fullMethod, call)
}

// WithNamespaceRedirect redirect the API call based on namespace name
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceRedirect(ctx context.Context, namespaceName namespace.Name, fullMethod string, _ any, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespace(namespaceName)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, namespaceEntry, fullMethod, call)
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) withRedirect(ctx context.Context, namespaceEntry *namespace.Namespace, fullMethod string, call func(string) error) error {
	targetDC, enableNamespaceNotActiveForwarding := policy.getTargetClusterAndIsNamespaceNotActiveAutoForwarding(ctx, namespaceEntry, fullMethod)

	err := call(targetDC)

	targetDC, ok := policy.isNamespaceNotActiveError(err)
	if !ok || !enableNamespaceNotActiveForwarding {
		return err
	}
	return call(targetDC)
}

// whitelisted reports whether fullMethod forwards under the selected-APIs policy.
func (policy *SelectedAPIsForwardingRedirectionPolicy) whitelisted(fullMethod string) bool {
	if _, ok := selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs[fullMethod]; ok {
		return true
	}
	_, ok := policy.additionalWhitelisted[fullMethod]
	return ok
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) isNamespaceNotActiveError(err error) (string, bool) {
	namespaceNotActiveErr, ok := err.(*serviceerror.NamespaceNotActive)
	if !ok {
		return "", false
	}
	return namespaceNotActiveErr.ActiveCluster, true
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) getTargetClusterAndIsNamespaceNotActiveAutoForwarding(ctx context.Context, namespaceEntry *namespace.Namespace, fullMethod string) (string, bool) {
	if !namespaceEntry.IsGlobalNamespace() {
		return policy.currentClusterName, false
	}

	if !policy.enabledForNS(namespaceEntry.Name().String()) {
		// do not do dc redirection if auto-forwarding dynamic config flag is not enabled
		return policy.currentClusterName, false
	}

	// Get routingKey from context (set by RoutingKeyInterceptor)
	routingKey := GetRoutingKeyFromContext(ctx)

	if policy.whitelisted(fullMethod) {
		// redirect if API is whitelisted
		return namespaceEntry.ActiveClusterName(routingKey), true
	}

	if policy.selectedAPIsOnly || policy.selectedAPIsOnlyForNS(namespaceEntry.Name().String()) {
		return policy.currentClusterName, false
	}

	return namespaceEntry.ActiveClusterName(routingKey), true
}

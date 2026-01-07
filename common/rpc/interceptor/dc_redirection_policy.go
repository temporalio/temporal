//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination dc_redirection_policy_mock.go

package interceptor

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
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
	// DCRedirectionPolicySelectedAPIsForwarding means forwarding the following APIs based namespace
	// 1. StartWorkflowExecution
	// 2. SignalWithStartWorkflowExecution
	// 3. SignalWorkflowExecution
	// 4. RequestCancelWorkflowExecution
	// 5. TerminateWorkflowExecution
	// 6. QueryWorkflow
	// please also reference selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs
	DCRedirectionPolicySelectedAPIsForwarding = "selected-apis-forwarding"

	// DCRedirectionPolicyAllAPIsForwarding means forwarding all APIs based on namespace active cluster
	DCRedirectionPolicyAllAPIsForwarding = "all-apis-forwarding"
)

type (
	// DCRedirectionPolicy is a DC redirection policy interface
	DCRedirectionPolicy interface {
		WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, apiName string, req any, call func(string) error) error
		WithNamespaceRedirect(ctx context.Context, namespaceName namespace.Name, apiName string, req any, call func(string) error) error
	}

	// NoopRedirectionPolicy is DC redirection policy which does nothing
	NoopRedirectionPolicy struct {
		currentClusterName string
	}

	// SelectedAPIsForwardingRedirectionPolicy is a DC redirection policy
	// which (based on namespace) forwards selected APIs calls to active cluster
	SelectedAPIsForwardingRedirectionPolicy struct {
		currentClusterName string
		enabledForNS       dynamicconfig.BoolPropertyFnWithNamespaceFilter
		namespaceRegistry  namespace.Registry
		enableForAllAPIs   bool
	}
)

// selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs contains a list of APIs which can be redirected
var selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs = map[string]struct{}{
	"StartWorkflowExecution":           {},
	"SignalWithStartWorkflowExecution": {},
	"SignalWorkflowExecution":          {},
	"RequestCancelWorkflowExecution":   {},
	"TerminateWorkflowExecution":       {},
	"QueryWorkflow":                    {},
}

// RedirectionPolicyGenerator generate corresponding redirection policy
func RedirectionPolicyGenerator(
	clusterMetadata cluster.Metadata,
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
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
		return NewSelectedAPIsForwardingPolicy(currentClusterName, enabledForNS, namespaceRegistry)
	case DCRedirectionPolicyAllAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewAllAPIsForwardingPolicy(currentClusterName, enabledForNS, namespaceRegistry)
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
	namespaceRegistry namespace.Registry,
) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		enabledForNS:       enabledForNS,
		namespaceRegistry:  namespaceRegistry,
	}
}

// NewAllAPIsForwardingPolicy creates a forwarding policy for all APIs based on namespace
func NewAllAPIsForwardingPolicy(
	currentClusterName string,
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		enabledForNS:       enabledForNS,
		namespaceRegistry:  namespaceRegistry,
		enableForAllAPIs:   true,
	}
}

// WithNamespaceIDRedirect redirect the API call based on namespace ID
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, apiName string, _ any, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, namespaceEntry, apiName, call)
}

// WithNamespaceRedirect redirect the API call based on namespace name
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceRedirect(ctx context.Context, namespaceName namespace.Name, apiName string, _ any, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespace(namespaceName)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, namespaceEntry, apiName, call)
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) withRedirect(ctx context.Context, namespaceEntry *namespace.Namespace, apiName string, call func(string) error) error {
	targetDC, enableNamespaceNotActiveForwarding := policy.getTargetClusterAndIsNamespaceNotActiveAutoForwarding(ctx, namespaceEntry, apiName)

	err := call(targetDC)

	targetDC, ok := policy.isNamespaceNotActiveError(err)
	if !ok || !enableNamespaceNotActiveForwarding {
		return err
	}
	return call(targetDC)
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) isNamespaceNotActiveError(err error) (string, bool) {
	namespaceNotActiveErr, ok := err.(*serviceerror.NamespaceNotActive)
	if !ok {
		return "", false
	}
	return namespaceNotActiveErr.ActiveCluster, true
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) getTargetClusterAndIsNamespaceNotActiveAutoForwarding(ctx context.Context, namespaceEntry *namespace.Namespace, apiName string) (string, bool) {
	if !namespaceEntry.IsGlobalNamespace() {
		return policy.currentClusterName, false
	}

	if !policy.enabledForNS(namespaceEntry.Name().String()) {
		// do not do dc redirection if auto-forwarding dynamic config flag is not enabled
		return policy.currentClusterName, false
	}

	// Get business ID from context (set by BusinessIDInterceptor)
	businessID := GetBusinessIDFromContext(ctx)

	if policy.enableForAllAPIs {
		return namespaceEntry.ActiveClusterName(businessID), true
	}

	_, ok := selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs[apiName]
	if !ok {
		// do not do dc redirection if API is not whitelisted
		return policy.currentClusterName, false
	}

	return namespaceEntry.ActiveClusterName(businessID), true
}

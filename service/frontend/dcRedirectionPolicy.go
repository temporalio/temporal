// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dcRedirectionPolicy_mock.go

package frontend

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
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
		WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, apiName string, call func(string) error) error
		WithNamespaceRedirect(ctx context.Context, namespace namespace.Name, apiName string, call func(string) error) error
	}

	// NoopRedirectionPolicy is DC redirection policy which does nothing
	NoopRedirectionPolicy struct {
		currentClusterName string
	}

	// SelectedAPIsForwardingRedirectionPolicy is a DC redirection policy
	// which (based on namespace) forwards selected APIs calls to active cluster
	SelectedAPIsForwardingRedirectionPolicy struct {
		currentClusterName string
		config             *Config
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
func RedirectionPolicyGenerator(clusterMetadata cluster.Metadata, config *Config,
	namespaceRegistry namespace.Registry, policy config.DCRedirectionPolicy) DCRedirectionPolicy {
	switch policy.Policy {
	case DCRedirectionPolicyDefault:
		// default policy, noop
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyNoop:
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicySelectedAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewSelectedAPIsForwardingPolicy(currentClusterName, config, namespaceRegistry)
	case DCRedirectionPolicyAllAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewAllAPIsForwardingPolicy(currentClusterName, config, namespaceRegistry)
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
func (policy *NoopRedirectionPolicy) WithNamespaceIDRedirect(_ context.Context, _ namespace.ID, _ string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// WithNamespaceRedirect redirect the API call based on namespace name
func (policy *NoopRedirectionPolicy) WithNamespaceRedirect(_ context.Context, _ namespace.Name, _ string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// NewSelectedAPIsForwardingPolicy creates a forwarding policy for selected APIs based on namespace
func NewSelectedAPIsForwardingPolicy(currentClusterName string, config *Config, namespaceRegistry namespace.Registry) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		config:             config,
		namespaceRegistry:  namespaceRegistry,
	}
}

// NewAllAPIsForwardingPolicy creates a forwarding policy for all APIs based on namespace
func NewAllAPIsForwardingPolicy(currentClusterName string, config *Config, namespaceRegistry namespace.Registry) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		config:             config,
		namespaceRegistry:  namespaceRegistry,
		enableForAllAPIs:   true,
	}
}

// WithNamespaceIDRedirect redirect the API call based on namespace ID
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, apiName string, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, namespaceEntry, apiName, call)
}

// WithNamespaceRedirect redirect the API call based on namespace name
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithNamespaceRedirect(ctx context.Context, namespace namespace.Name, apiName string, call func(string) error) error {
	namespaceEntry, err := policy.namespaceRegistry.GetNamespace(namespace)
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

	if len(namespaceEntry.ClusterNames()) == 1 {
		// do not do dc redirection if namespace is only targeting at 1 dc (effectively local namespace)
		return policy.currentClusterName, false
	}

	if !policy.config.EnableNamespaceNotActiveAutoForwarding(namespaceEntry.Name().String()) {
		// do not do dc redirection if auto-forwarding dynamic config flag is not enabled
		return policy.currentClusterName, false
	}

	if policy.enableForAllAPIs {
		return namespaceEntry.ActiveClusterName(), true
	}

	_, ok := selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs[apiName]
	if !ok {
		// do not do dc redirection if API is not whitelisted
		return policy.currentClusterName, false
	}

	return namespaceEntry.ActiveClusterName(), true
}

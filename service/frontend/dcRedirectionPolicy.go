// Copyright (c) 2017 Uber Technologies, Inc.
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

package frontend

import (
	"context"
	"fmt"

	"go.uber.org/yarpc"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/service/config"
)

const (
	// DCRedirectionPolicyDefault means no redirection
	DCRedirectionPolicyDefault = ""
	// DCRedirectionPolicyNoop means no redirection
	DCRedirectionPolicyNoop = "noop"
	// DCRedirectionPolicySelectedAPIsForwarding means forwarding the following APIs based domain
	// 1. StartWorkflowExecution
	// 2. SignalWithStartWorkflowExecution
	// 3. SignalWorkflowExecution
	// 4. RequestCancelWorkflowExecution
	// 5. TerminateWorkflowExecution
	// please also reference selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs
	DCRedirectionPolicySelectedAPIsForwarding = "selected-apis-forwarding"
)

type (
	// DCRedirectionPolicy is a DC redirection policy interface
	DCRedirectionPolicy interface {
		WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error
		WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error
	}

	// NoopRedirectionPolicy is DC redirection policy which does nothing
	NoopRedirectionPolicy struct {
		currentClusterName string
	}

	// SelectedAPIsForwardingRedirectionPolicy is a DC redirection policy
	// which (based on domain) forwards selected APIs calls to active cluster
	SelectedAPIsForwardingRedirectionPolicy struct {
		currentClusterName string
		config             *Config
		domainCache        cache.DomainCache
	}
)

// selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs contains a list of APIs which can be redirected
var selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs = map[string]struct{}{
	"StartWorkflowExecution":           {},
	"SignalWithStartWorkflowExecution": {},
	"SignalWorkflowExecution":          {},
	"RequestCancelWorkflowExecution":   {},
	"TerminateWorkflowExecution":       {},
}

// RedirectionPolicyGenerator generate corresponding redirection policy
func RedirectionPolicyGenerator(clusterMetadata cluster.Metadata, config *Config,
	domainCache cache.DomainCache, policy config.DCRedirectionPolicy) DCRedirectionPolicy {
	switch policy.Policy {
	case DCRedirectionPolicyDefault:
		// default policy, noop
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyNoop:
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicySelectedAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return NewSelectedAPIsForwardingPolicy(currentClusterName, config, domainCache)
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

// WithDomainIDRedirect redirect the API call based on domain ID
func (policy *NoopRedirectionPolicy) WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// WithDomainNameRedirect redirect the API call based on domain name
func (policy *NoopRedirectionPolicy) WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// NewSelectedAPIsForwardingPolicy creates a forwarding policy for selected APIs based on domain
func NewSelectedAPIsForwardingPolicy(currentClusterName string, config *Config, domainCache cache.DomainCache) *SelectedAPIsForwardingRedirectionPolicy {
	return &SelectedAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		config:             config,
		domainCache:        domainCache,
	}
}

// WithDomainIDRedirect redirect the API call based on domain ID
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error {
	domainEntry, err := policy.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, domainEntry, apiName, call)
}

// WithDomainNameRedirect redirect the API call based on domain name
func (policy *SelectedAPIsForwardingRedirectionPolicy) WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error {
	domainEntry, err := policy.domainCache.GetDomain(domainName)
	if err != nil {
		return err
	}
	return policy.withRedirect(ctx, domainEntry, apiName, call)
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) withRedirect(ctx context.Context, domainEntry *cache.DomainCacheEntry, apiName string, call func(string) error) error {
	targetDC, enableDomainNotActiveForwarding := policy.getTargetClusterAndIsDomainNotActiveAutoForwarding(ctx, domainEntry, apiName)

	err := call(targetDC)

	targetDC, ok := policy.isDomainNotActiveError(err)
	if !ok || !enableDomainNotActiveForwarding {
		return err
	}
	return call(targetDC)
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) isDomainNotActiveError(err error) (string, bool) {
	domainNotActiveErr, ok := err.(*shared.DomainNotActiveError)
	if !ok {
		return "", false
	}
	return domainNotActiveErr.ActiveCluster, true
}

func (policy *SelectedAPIsForwardingRedirectionPolicy) getTargetClusterAndIsDomainNotActiveAutoForwarding(ctx context.Context, domainEntry *cache.DomainCacheEntry, apiName string) (string, bool) {
	if !domainEntry.IsGlobalDomain() {
		return policy.currentClusterName, false
	}

	if len(domainEntry.GetReplicationConfig().Clusters) == 1 {
		// do not do dc redirection if domain is only targeting at 1 dc (effectively local domain)
		return policy.currentClusterName, false
	}

	call := yarpc.CallFromContext(ctx)
	enforceDCRedirection := call.Header(common.EnforceDCRedirection)
	if !policy.config.EnableDomainNotActiveAutoForwarding(domainEntry.GetInfo().Name) && enforceDCRedirection != "true" {
		// do not do dc redirection if auto-forwarding dynamic config and EnforceDCRedirection context flag is not enabled
		return policy.currentClusterName, false
	}

	_, ok := selectedAPIsForwardingRedirectionPolicyWhitelistedAPIs[apiName]
	if !ok {
		// do not do dc redirection if API is not whitelisted
		return policy.currentClusterName, false
	}

	return domainEntry.GetReplicationConfig().ActiveClusterName, true
}

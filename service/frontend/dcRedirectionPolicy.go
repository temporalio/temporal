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
	"fmt"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/service/config"
)

const (
	// DCRedirectionPolicyDefault means no redirection
	DCRedirectionPolicyDefault = ""
	// DCRedirectionPolicyNoop means no redirection
	DCRedirectionPolicyNoop = "noop"
	// DCRedirectionPolicyForwarding means forwarding from an DC to another DC
	DCRedirectionPolicyForwarding = "forwarding"
)

type (
	// DCRedirectionPolicy is DC redirection policy interface
	DCRedirectionPolicy interface {
		GetTargetDatacenterByName(domainName string) (string, error)
		GetTargetDatacenterByID(domainName string) (string, error)
	}

	// NoopRedirectionPolicy is DC redirection policy which does nothing
	NoopRedirectionPolicy struct {
		currentClusteName string
	}

	// ForwardingDCRedirectionPolicy is DC redirection policy which forwards
	// API calls if domain is effective global, fromDC is current cluster,
	// fromDC is not in replication config and toDC is in replication config,
	ForwardingDCRedirectionPolicy struct {
		fromDC      string
		toDC        string
		domainCache cache.DomainCache
	}
)

// RedirectionPolicyGenerator generate corresponding redirection policy
func RedirectionPolicyGenerator(clusterMetadata cluster.Metadata,
	domainCache cache.DomainCache, policy config.DCRedirectionPolicy) DCRedirectionPolicy {
	switch policy.Policy {
	case DCRedirectionPolicyDefault:
		// default policy, noop
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyNoop:
		return NewNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		clusterAddress := clusterMetadata.GetAllClientAddress()
		if _, ok := clusterAddress[policy.ToDC]; !ok {
			panic(fmt.Sprintf("Incorrect to DC: %v", policy.ToDC))
		}
		return NewForwardingDCRedirectionPolicy(
			currentClusterName, policy.ToDC, domainCache,
		)
	default:
		panic(fmt.Sprintf("Unknown DC redirection policy %v", policy.Policy))
	}
}

// NewNoopRedirectionPolicy is DC redirection policy which does nothing
func NewNoopRedirectionPolicy(currentClusteName string) *NoopRedirectionPolicy {
	return &NoopRedirectionPolicy{
		currentClusteName: currentClusteName,
	}
}

// GetTargetDatacenterByName get target cluster name by domain Name
func (policy *NoopRedirectionPolicy) GetTargetDatacenterByName(domainName string) (string, error) {
	return policy.currentClusteName, nil
}

// GetTargetDatacenterByID get target cluster name by domain ID
func (policy *NoopRedirectionPolicy) GetTargetDatacenterByID(domainID string) (string, error) {
	return policy.currentClusteName, nil
}

// NewForwardingDCRedirectionPolicy creates a datacenter redirection policy forwarding API calls
func NewForwardingDCRedirectionPolicy(fromDC string, toDC string, domainCache cache.DomainCache) *ForwardingDCRedirectionPolicy {
	return &ForwardingDCRedirectionPolicy{
		fromDC:      fromDC,
		toDC:        toDC,
		domainCache: domainCache,
	}
}

// GetTargetDatacenterByName get target cluster name by domain Name
func (policy *ForwardingDCRedirectionPolicy) GetTargetDatacenterByName(domainName string) (string, error) {
	domainEntry, err := policy.domainCache.GetDomain(domainName)
	if err != nil {
		return "", err
	}

	return policy.getTargetDatacenter(domainEntry), nil
}

// GetTargetDatacenterByID get target cluster name by domain ID
func (policy *ForwardingDCRedirectionPolicy) GetTargetDatacenterByID(domainID string) (string, error) {
	domainEntry, err := policy.domainCache.GetDomainByID(domainID)
	if err != nil {
		return "", err
	}

	return policy.getTargetDatacenter(domainEntry), nil
}

func (policy *ForwardingDCRedirectionPolicy) getTargetDatacenter(domainEntry *cache.DomainCacheEntry) string {
	if !domainEntry.IsGlobalDomain() {
		return policy.fromDC
	}

	if len(domainEntry.GetReplicationConfig().Clusters) == 1 {
		// do not do dc redirection if domain is only targeting at 1 dc (effectively local domain)
		return policy.fromDC
	}

	replicationClusterNames := map[string]struct{}{}
	for _, clusterConfig := range domainEntry.GetReplicationConfig().Clusters {
		replicationClusterNames[clusterConfig.ClusterName] = struct{}{}
	}

	_, containsFromDC := replicationClusterNames[policy.fromDC]
	_, containsToDC := replicationClusterNames[policy.toDC]

	if !containsFromDC && containsToDC {
		return policy.toDC
	}
	return policy.fromDC
}

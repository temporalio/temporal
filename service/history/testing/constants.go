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

package testing

import (
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

var (
	// Version is the workflow version for test
	Version = int64(1234)
	// DomainID is the domainID for test
	DomainID = "deadbeef-0123-4567-890a-bcdef0123456"
	// DomainName is the domainName for test
	DomainName = "some random domain name"
	// ParentDomainID is the parentDomainID for test
	ParentDomainID = "deadbeef-0123-4567-890a-bcdef0123457"
	// ParentDomainName is the parentDomainName for test
	ParentDomainName = "some random parent domain name"
	// TargetDomainID is the targetDomainID for test
	TargetDomainID = "deadbeef-0123-4567-890a-bcdef0123458"
	// TargetDomainName is the targetDomainName for test
	TargetDomainName = "some random target domain name"
	// ChildDomainID is the childDomainID for test
	ChildDomainID = "deadbeef-0123-4567-890a-bcdef0123459"
	// ChildDomainName is the childDomainName for test
	ChildDomainName = "some random child domain name"
	// WorkflowID is the workflowID for test
	WorkflowID = "random-workflow-id"
	// RunID is the workflow runID for test
	RunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

	// LocalDomainEntry is the local domain cache entry for test
	LocalDomainEntry = cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: DomainID, Name: DomainName},
		&persistence.DomainConfig{Retention: 1},
		cluster.TestCurrentClusterName,
		nil,
	)

	// GlobalDomainEntry is the global domain cache entry for test
	GlobalDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: DomainID, Name: DomainName},
		&persistence.DomainConfig{
			Retention:                1,
			VisibilityArchivalStatus: workflow.ArchivalStatusEnabled,
			VisibilityArchivalURI:    "test:///visibility/archival",
		},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		Version,
		nil,
	)

	// GlobalParentDomainEntry is the global parent domain cache entry for test
	GlobalParentDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: ParentDomainID, Name: ParentDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		Version,
		nil,
	)

	// GlobalTargetDomainEntry is the global target domain cache entry for test
	GlobalTargetDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TargetDomainID, Name: TargetDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		Version,
		nil,
	)

	// GlobalChildDomainEntry is the global child domain cache entry for test
	GlobalChildDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: ChildDomainID, Name: ChildDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		Version,
		nil,
	)
)

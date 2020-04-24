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

package constants

import (
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

var (
	// TestVersion is the workflow version for test
	TestVersion = int64(1234)
	// TestDomainID is the domainID for test
	TestDomainID = "deadbeef-0123-4567-890a-bcdef0123456"
	// TestDomainName is the domainName for test
	TestDomainName = "some random domain name"
	// TestParentDomainID is the parentDomainID for test
	TestParentDomainID = "deadbeef-0123-4567-890a-bcdef0123457"
	// TestParentDomainName is the parentDomainName for test
	TestParentDomainName = "some random parent domain name"
	// TestTargetDomainID is the targetDomainID for test
	TestTargetDomainID = "deadbeef-0123-4567-890a-bcdef0123458"
	// TestTargetDomainName is the targetDomainName for test
	TestTargetDomainName = "some random target domain name"
	// TestChildDomainID is the childDomainID for test
	TestChildDomainID = "deadbeef-0123-4567-890a-bcdef0123459"
	// TestChildDomainName is the childDomainName for test
	TestChildDomainName = "some random child domain name"
	// TestWorkflowID is the workflowID for test
	TestWorkflowID = "random-workflow-id"
	// TestRunID is the workflow runID for test
	TestRunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

	// TestLocalDomainEntry is the local domain cache entry for test
	TestLocalDomainEntry = cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TestDomainID, Name: TestDomainName},
		&persistence.DomainConfig{Retention: 1},
		cluster.TestCurrentClusterName,
		nil,
	)

	// TestGlobalDomainEntry is the global domain cache entry for test
	TestGlobalDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TestDomainID, Name: TestDomainName},
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
		TestVersion,
		nil,
	)

	// TestGlobalParentDomainEntry is the global parent domain cache entry for test
	TestGlobalParentDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TestParentDomainID, Name: TestParentDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TestVersion,
		nil,
	)

	// TestGlobalTargetDomainEntry is the global target domain cache entry for test
	TestGlobalTargetDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TestTargetDomainID, Name: TestTargetDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TestVersion,
		nil,
	)

	// TestGlobalChildDomainEntry is the global child domain cache entry for test
	TestGlobalChildDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: TestChildDomainID, Name: TestChildDomainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		TestVersion,
		nil,
	)
)

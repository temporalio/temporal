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

package tests

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
)

var Version = int64(1234)
var NamespaceID = "deadbeef-0123-4567-890a-bcdef0123456"
var Namespace = "some random namespace name"
var ParentNamespaceID = "deadbeef-0123-4567-890a-bcdef0123457"
var ParentNamespace = "some random parent namespace name"
var TargetNamespaceID = "deadbeef-0123-4567-890a-bcdef0123458"
var TargetNamespace = "some random target namespace name"
var ChildNamespaceID = "deadbeef-0123-4567-890a-bcdef0123459"
var ChildNamespace = "some random child namespace name"
var WorkflowID = "random-workflow-id"
var RunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

var LocalNamespaceEntry = cache.NewLocalNamespaceCacheEntryForTest(
	&persistencespb.NamespaceInfo{Id: NamespaceID, Name: Namespace},
	&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
	cluster.TestCurrentClusterName,
	nil,
)

var GlobalNamespaceEntry = cache.NewGlobalNamespaceCacheEntryForTest(
	&persistencespb.NamespaceInfo{Id: NamespaceID, Name: Namespace},
	&persistencespb.NamespaceConfig{
		Retention:               timestamp.DurationFromDays(1),
		VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
		VisibilityArchivalUri:   "test:///visibility/archival",
	},
	&persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	},
	Version,
	nil,
)

var GlobalParentNamespaceEntry = cache.NewGlobalNamespaceCacheEntryForTest(
	&persistencespb.NamespaceInfo{Id: ParentNamespaceID, Name: ParentNamespace},
	&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
	&persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	},
	Version,
	nil,
)

var GlobalTargetNamespaceEntry = cache.NewGlobalNamespaceCacheEntryForTest(
	&persistencespb.NamespaceInfo{Id: TargetNamespaceID, Name: TargetNamespace},
	&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
	&persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	},
	Version,
	nil,
)

var GlobalChildNamespaceEntry = cache.NewGlobalNamespaceCacheEntryForTest(
	&persistencespb.NamespaceInfo{Id: ChildNamespaceID, Name: ChildNamespace},
	&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
	&persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	},
	Version,
	nil,
)

func NewDynamicConfig() *configs.Config {
	dc := dynamicconfig.NewNoopCollection()
	config := configs.NewConfig(dc, 1, false, "")
	// reduce the duration of long poll to increase test speed
	config.LongPollExpirationInterval = dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.HistoryLongPollExpirationInterval, 10*time.Second)
	return config
}

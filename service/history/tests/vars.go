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

	namespacepb "go.temporal.io/api/namespace/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
)

var (
	Version = int64(1234)

	NamespaceID        = namespace.ID("deadbeef-0123-4567-890a-bcdef0123456")
	Namespace          = namespace.Name("mock namespace name")
	ParentNamespaceID  = namespace.ID("deadbeef-0123-4567-890a-bcdef0123457")
	ParentNamespace    = namespace.Name("mock parent namespace name")
	TargetNamespaceID  = namespace.ID("deadbeef-0123-4567-890a-bcdef0123458")
	TargetNamespace    = namespace.Name("mock target namespace name")
	ChildNamespaceID   = namespace.ID("deadbeef-0123-4567-890a-bcdef0123459")
	ChildNamespace     = namespace.Name("mock child namespace name")
	StandbyNamespaceID = namespace.ID("deadbeef-0123-4567-890a-bcdef0123460")
	StandbyNamespace   = namespace.Name("mock standby namespace name")
	MissedNamespaceID  = namespace.ID("missed-namespace-id")
	WorkflowID         = "mock-workflow-id"
	RunID              = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

	LocalNamespaceEntry = namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: NamespaceID.String(), Name: Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(1),
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{},
			},
		},
		cluster.TestCurrentClusterName,
	)

	GlobalNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: NamespaceID.String(), Name: Namespace.String()},
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
	)

	GlobalParentNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: ParentNamespaceID.String(), Name: ParentNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalTargetNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: TargetNamespaceID.String(), Name: TargetNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalStandbyNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: StandbyNamespaceID.String(), Name: StandbyNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalChildNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: ChildNamespaceID.String(), Name: ChildNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	CreateWorkflowExecutionResponse = &persistence.CreateWorkflowExecutionResponse{
		NewMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}

	GetWorkflowExecutionResponse = &persistence.GetWorkflowExecutionResponse{
		MutableStateStats: persistence.MutableStateStatistics{},
	}

	UpdateWorkflowExecutionResponse = &persistence.UpdateWorkflowExecutionResponse{
		UpdateMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
		NewMutableStateStats: nil,
	}
)

func NewDynamicConfig() *configs.Config {
	dc := dynamicconfig.NewNoopCollection()
	config := configs.NewConfig(dc, 1, false, "")
	// reduce the duration of long poll to increase test speed
	config.LongPollExpirationInterval = dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.HistoryLongPollExpirationInterval, 10*time.Second)
	config.EnableActivityEagerExecution = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.NamespaceCacheRefreshInterval = dynamicconfig.GetDurationPropertyFn(time.Second)
	return config
}

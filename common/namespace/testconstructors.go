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

package namespace

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

//TODO: delete this whole file and transition usages to FromPersistentState

// NewLocalNamespaceForTest returns an entry with test data
func NewLocalNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	targetCluster string,
) *Namespace {
	return &Namespace{
		info:              derefInfo(info),
		config:            derefConfig(config),
		isGlobalNamespace: false,
		replicationConfig: persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
			Clusters:          []string{targetCluster},
		},
		failoverVersion: common.EmptyVersion,
	}
}

// NewNamespaceForTest returns an entry with test data
func NewNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	isGlobalNamespace bool,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	return &Namespace{
		info:              derefInfo(info),
		config:            derefConfig(config),
		isGlobalNamespace: isGlobalNamespace,
		replicationConfig: derefRepConfig(repConfig),
		failoverVersion:   failoverVersion,
	}
}

// newGlobalNamespaceForTest returns an entry with test data
func NewGlobalNamespaceForTest(
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	repConfig *persistencespb.NamespaceReplicationConfig,
	failoverVersion int64,
) *Namespace {
	return &Namespace{
		info:              derefInfo(info),
		config:            derefConfig(config),
		isGlobalNamespace: true,
		replicationConfig: derefRepConfig(repConfig),
		failoverVersion:   failoverVersion,
	}
}

func derefInfo(ptr *persistencespb.NamespaceInfo) persistencespb.NamespaceInfo {
	if ptr == nil {
		return persistencespb.NamespaceInfo{}
	}
	return *ptr
}

func derefConfig(ptr *persistencespb.NamespaceConfig) persistencespb.NamespaceConfig {
	if ptr == nil {
		return persistencespb.NamespaceConfig{}
	}
	return *ptr
}

func derefRepConfig(ptr *persistencespb.NamespaceReplicationConfig) persistencespb.NamespaceReplicationConfig {
	if ptr == nil {
		return persistencespb.NamespaceReplicationConfig{}
	}
	return *ptr
}

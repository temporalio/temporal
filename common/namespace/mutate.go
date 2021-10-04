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
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/server/common/persistence"
)

type entryMutationFunc func(*persistence.GetNamespaceResponse)

func (f entryMutationFunc) apply(ns *persistence.GetNamespaceResponse) {
	f(ns)
}

// WithActiveCluster assigns the active cluster to a CacheEntry during a Clone
// operation.
func WithActiveCluster(name string) EntryMutation {
	return entryMutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.ReplicationConfig.ActiveClusterName = name
		})
}

// WithBadBinary adds a bad binary checksum to a CacheEntry during a Clone
// operation.
func WithBadBinary(chksum string) EntryMutation {
	return entryMutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.Config.BadBinaries.Binaries[chksum] =
				&namespacepb.BadBinaryInfo{}
		})
}

// WithID assigns the ID to a CacheEntry during a Clone operation.
func WithID(id string) EntryMutation {
	return entryMutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.Info.Id = id
		})
}

// WithGlobalFlag sets wether or not this CacheEntry is global.
func WithGlobalFlag(b bool) EntryMutation {
	return entryMutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.IsGlobalNamespace = b
		})
}

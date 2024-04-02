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
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/persistence"
)

type mutationFunc func(*persistence.GetNamespaceResponse)

func (f mutationFunc) apply(ns *persistence.GetNamespaceResponse) {
	f(ns)
}

// WithActiveCluster assigns the active cluster to a Namespace during a Clone
// operation.
func WithActiveCluster(name string) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.ReplicationConfig.ActiveClusterName = name
		})
}

// WithBadBinary adds a bad binary checksum to a Namespace during a Clone
// operation.
func WithBadBinary(chksum string) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			if ns.Namespace.Config.BadBinaries.Binaries == nil {
				ns.Namespace.Config.BadBinaries.Binaries = make(map[string]*namespacepb.BadBinaryInfo)
			}
			ns.Namespace.Config.BadBinaries.Binaries[chksum] =
				&namespacepb.BadBinaryInfo{}
		})
}

// WithID assigns the ID to a Namespace during a Clone operation.
func WithID(id string) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.Info.Id = id
		})
}

// WithGlobalFlag sets whether or not this Namespace is global.
func WithGlobalFlag(b bool) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.IsGlobalNamespace = b
		})
}

// WithRetention assigns the retention duration to a Namespace during a Clone
// operation.
func WithRetention(dur *durationpb.Duration) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			ns.Namespace.Config.Retention = dur
		})
}

// WithData adds a key-value pair to a Namespace during a Clone operation.
func WithData(key, value string) Mutation {
	return mutationFunc(
		func(ns *persistence.GetNamespaceResponse) {
			if ns.Namespace.Info.Data == nil {
				ns.Namespace.Info.Data = make(map[string]string)
			}
			ns.Namespace.Info.Data[key] = value
		})
}

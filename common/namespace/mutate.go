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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
)

type mutationFunc func(*Namespace)

func (f mutationFunc) apply(ns *Namespace) {
	f(ns)
}

// WithActiveCluster assigns the active cluster to a Namespace during a Clone
// operation.
func WithActiveCluster(name string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.replicationConfig.ActiveClusterName = name
		})
}

// WithBadBinary adds a bad binary checksum to a Namespace during a Clone
// operation.
func WithBadBinary(chksum string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			if ns.config.BadBinaries.Binaries == nil {
				ns.config.BadBinaries.Binaries = make(map[string]*namespacepb.BadBinaryInfo)
			}
			ns.config.BadBinaries.Binaries[chksum] =
				&namespacepb.BadBinaryInfo{}
		})
}

// WithID assigns the ID to a Namespace during a Clone operation.
func WithID(id string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.info.Id = id
		})
}

// WithGlobalFlag sets whether or not this Namespace is global.
func WithGlobalFlag(b bool) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.isGlobalNamespace = b
		})
}

// WithNotificationVersion assigns a notification version to the Namespace.
func WithNotificationVersion(v int64) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.notificationVersion = v
		})
}

// WithRetention assigns the retention duration to a Namespace during a Clone
// operation.
func WithRetention(dur *durationpb.Duration) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.config.Retention = dur
		})
}

// WithData adds a key-value pair to a Namespace during a Clone operation.
func WithData(key, value string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			if ns.info.Data == nil {
				ns.info.Data = make(map[string]string)
			}
			ns.info.Data[key] = value
		})
}

func WithPretendLocalNamespace(localClusterName string) Mutation {
	return mutationFunc(
		func(ns *Namespace) {
			ns.isGlobalNamespace = false
			ns.replicationConfig = &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: localClusterName,
				Clusters:          []string{localClusterName},
			}
			ns.failoverVersion = common.EmptyVersion
		})

}

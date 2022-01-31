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
	"go.temporal.io/server/common/persistence"
)

type (
	PretendAsLocalNamespace struct {
		localClusterName string
	}
)

var _ Mutation = PretendAsLocalNamespace{}

// NewPretendAsLocalNamespace create a Mutation which update namespace replication
// config as if this namespace is local
func NewPretendAsLocalNamespace(
	localClusterName string,
) PretendAsLocalNamespace {
	return PretendAsLocalNamespace{
		localClusterName: localClusterName,
	}
}

func (c PretendAsLocalNamespace) apply(response *persistence.GetNamespaceResponse) {
	response.IsGlobalNamespace = false
	response.Namespace.ReplicationConfig = &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: c.localClusterName,
		Clusters:          persistence.GetOrUseDefaultClusters(c.localClusterName, nil),
	}
	response.Namespace.FailoverVersion = common.EmptyVersion
}

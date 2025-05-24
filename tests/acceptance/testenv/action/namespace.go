// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
// FITNESS FOR A PARTICULAR PURPOSE AND NGetONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package action

import (
	"time"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CreateNamespace struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.Namespace]
	Name               stamp.Gen[stamp.ID]
	ID                 stamp.Gen[stamp.ID]
	NamespaceRetention stamp.Gen[time.Duration]
}

func (c CreateNamespace) Next(ctx stamp.GenContext) *persistence.CreateNamespaceRequest {
	return &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          namespace.ID(uuid.New()).String(), // TODO use generator
				Name:        string(c.Name.Next(ctx.AllowRandom())),
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "namespace for acceptance tests",
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   durationpb.New(c.NamespaceRetention.NextOrDefault(ctx, 24*time.Hour)),
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: string(c.GetActor().GetID()),
				Clusters: []string{
					string(c.GetActor().GetID()),
				},
			},
		},
	}
}

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

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

type (
	faultInjectionClusterMetadataStore struct {
		baseStore persistence.ClusterMetadataStore
		generator faultGenerator
	}
)

func newFaultInjectionClusterMetadataStore(
	baseStore persistence.ClusterMetadataStore,
	generator faultGenerator,
) *faultInjectionClusterMetadataStore {
	return &faultInjectionClusterMetadataStore{
		baseStore: baseStore,
		generator: generator,
	}
}

func (c *faultInjectionClusterMetadataStore) Close() {
	c.baseStore.Close()
}

func (c *faultInjectionClusterMetadataStore) GetName() string {
	return c.baseStore.GetName()
}

func (c *faultInjectionClusterMetadataStore) ListClusterMetadata(
	ctx context.Context,
	request *persistence.InternalListClusterMetadataRequest,
) (*persistence.InternalListClusterMetadataResponse, error) {
	return inject1(c.generator.generate(), func() (*persistence.InternalListClusterMetadataResponse, error) {
		return c.baseStore.ListClusterMetadata(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) GetClusterMetadata(
	ctx context.Context,
	request *persistence.InternalGetClusterMetadataRequest,
) (*persistence.InternalGetClusterMetadataResponse, error) {
	return inject1(c.generator.generate(), func() (*persistence.InternalGetClusterMetadataResponse, error) {
		return c.baseStore.GetClusterMetadata(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *persistence.InternalSaveClusterMetadataRequest,
) (bool, error) {
	return inject1(c.generator.generate(), func() (bool, error) {
		return c.baseStore.SaveClusterMetadata(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) DeleteClusterMetadata(
	ctx context.Context,
	request *persistence.InternalDeleteClusterMetadataRequest,
) error {
	return inject0(c.generator.generate(), func() error {
		return c.baseStore.DeleteClusterMetadata(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *persistence.GetClusterMembersRequest,
) (*persistence.GetClusterMembersResponse, error) {
	return inject1(c.generator.generate(), func() (*persistence.GetClusterMembersResponse, error) {
		return c.baseStore.GetClusterMembers(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *persistence.UpsertClusterMembershipRequest,
) error {
	return inject0(c.generator.generate(), func() error {
		return c.baseStore.UpsertClusterMembership(ctx, request)
	})
}

func (c *faultInjectionClusterMetadataStore) PruneClusterMembership(
	ctx context.Context,
	request *persistence.PruneClusterMembershipRequest,
) error {
	return inject0(c.generator.generate(), func() error {
		return c.baseStore.PruneClusterMembership(ctx, request)
	})
}

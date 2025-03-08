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

// Code generated by gowrap. DO NOT EDIT.
// template: gowrap_template
// gowrap: http://github.com/hexdigest/gowrap

package faultinjection

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i ClusterMetadataStore -t gowrap_template -o cluster_metadata_store_gen.go -l ""

import (
	"context"

	_sourcePersistence "go.temporal.io/server/common/persistence"
)

type (
	// faultInjectionClusterMetadataStore implements ClusterMetadataStore interface with fault injection.
	faultInjectionClusterMetadataStore struct {
		_sourcePersistence.ClusterMetadataStore
		generator faultGenerator
	}
)

// newFaultInjectionClusterMetadataStore returns faultInjectionClusterMetadataStore.
func newFaultInjectionClusterMetadataStore(
	baseStore _sourcePersistence.ClusterMetadataStore,
	generator faultGenerator,
) *faultInjectionClusterMetadataStore {
	return &faultInjectionClusterMetadataStore{
		ClusterMetadataStore: baseStore,
		generator:            generator,
	}
}

// DeleteClusterMetadata wraps ClusterMetadataStore.DeleteClusterMetadata.
func (d faultInjectionClusterMetadataStore) DeleteClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalDeleteClusterMetadataRequest) (err error) {
	err = d.generator.generate("DeleteClusterMetadata").inject(func() error {
		err = d.ClusterMetadataStore.DeleteClusterMetadata(ctx, request)
		return err
	})
	return
}

// GetClusterMembers wraps ClusterMetadataStore.GetClusterMembers.
func (d faultInjectionClusterMetadataStore) GetClusterMembers(ctx context.Context, request *_sourcePersistence.GetClusterMembersRequest) (gp1 *_sourcePersistence.GetClusterMembersResponse, err error) {
	err = d.generator.generate("GetClusterMembers").inject(func() error {
		gp1, err = d.ClusterMetadataStore.GetClusterMembers(ctx, request)
		return err
	})
	return
}

// GetClusterMetadata wraps ClusterMetadataStore.GetClusterMetadata.
func (d faultInjectionClusterMetadataStore) GetClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalGetClusterMetadataRequest) (ip1 *_sourcePersistence.InternalGetClusterMetadataResponse, err error) {
	err = d.generator.generate("GetClusterMetadata").inject(func() error {
		ip1, err = d.ClusterMetadataStore.GetClusterMetadata(ctx, request)
		return err
	})
	return
}

// ListClusterMetadata wraps ClusterMetadataStore.ListClusterMetadata.
func (d faultInjectionClusterMetadataStore) ListClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalListClusterMetadataRequest) (ip1 *_sourcePersistence.InternalListClusterMetadataResponse, err error) {
	err = d.generator.generate("ListClusterMetadata").inject(func() error {
		ip1, err = d.ClusterMetadataStore.ListClusterMetadata(ctx, request)
		return err
	})
	return
}

// PruneClusterMembership wraps ClusterMetadataStore.PruneClusterMembership.
func (d faultInjectionClusterMetadataStore) PruneClusterMembership(ctx context.Context, request *_sourcePersistence.PruneClusterMembershipRequest) (err error) {
	err = d.generator.generate("PruneClusterMembership").inject(func() error {
		err = d.ClusterMetadataStore.PruneClusterMembership(ctx, request)
		return err
	})
	return
}

// SaveClusterMetadata wraps ClusterMetadataStore.SaveClusterMetadata.
func (d faultInjectionClusterMetadataStore) SaveClusterMetadata(ctx context.Context, request *_sourcePersistence.InternalSaveClusterMetadataRequest) (b1 bool, err error) {
	err = d.generator.generate("SaveClusterMetadata").inject(func() error {
		b1, err = d.ClusterMetadataStore.SaveClusterMetadata(ctx, request)
		return err
	})
	return
}

// UpsertClusterMembership wraps ClusterMetadataStore.UpsertClusterMembership.
func (d faultInjectionClusterMetadataStore) UpsertClusterMembership(ctx context.Context, request *_sourcePersistence.UpsertClusterMembershipRequest) (err error) {
	err = d.generator.generate("UpsertClusterMembership").inject(func() error {
		err = d.ClusterMetadataStore.UpsertClusterMembership(ctx, request)
		return err
	})
	return
}

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

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i ShardStore -t gowrap_template -o shard_store_gen.go -l ""

import (
	"context"

	_sourcePersistence "go.temporal.io/server/common/persistence"
)

type (
	// faultInjectionShardStore implements ShardStore interface with fault injection.
	faultInjectionShardStore struct {
		_sourcePersistence.ShardStore
		generator faultGenerator
	}
)

// newFaultInjectionShardStore returns faultInjectionShardStore.
func newFaultInjectionShardStore(
	baseStore _sourcePersistence.ShardStore,
	generator faultGenerator,
) *faultInjectionShardStore {
	return &faultInjectionShardStore{
		ShardStore: baseStore,
		generator:  generator,
	}
}

// AssertShardOwnership wraps ShardStore.AssertShardOwnership.
func (_d faultInjectionShardStore) AssertShardOwnership(ctx context.Context, request *_sourcePersistence.AssertShardOwnershipRequest) (err error) {
	err = _d.generator.generate("AssertShardOwnership").inject(func() error {
		err = _d.ShardStore.AssertShardOwnership(ctx, request)
		return err
	})
	return
}

// GetOrCreateShard wraps ShardStore.GetOrCreateShard.
func (_d faultInjectionShardStore) GetOrCreateShard(ctx context.Context, request *_sourcePersistence.InternalGetOrCreateShardRequest) (ip1 *_sourcePersistence.InternalGetOrCreateShardResponse, err error) {
	err = _d.generator.generate("GetOrCreateShard").inject(func() error {
		ip1, err = _d.ShardStore.GetOrCreateShard(ctx, request)
		return err
	})
	return
}

// UpdateShard wraps ShardStore.UpdateShard.
func (_d faultInjectionShardStore) UpdateShard(ctx context.Context, request *_sourcePersistence.InternalUpdateShardRequest) (err error) {
	err = _d.generator.generate("UpdateShard").inject(func() error {
		err = _d.ShardStore.UpdateShard(ctx, request)
		return err
	})
	return
}

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

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i NexusEndpointStore -t gowrap_template -o nexus_endpoint_store_gen.go -l ""

import (
	"context"

	_sourcePersistence "go.temporal.io/server/common/persistence"
)

type (
	// faultInjectionNexusEndpointStore implements NexusEndpointStore interface with fault injection.
	faultInjectionNexusEndpointStore struct {
		_sourcePersistence.NexusEndpointStore
		generator faultGenerator
	}
)

// newFaultInjectionNexusEndpointStore returns faultInjectionNexusEndpointStore.
func newFaultInjectionNexusEndpointStore(
	baseStore _sourcePersistence.NexusEndpointStore,
	generator faultGenerator,
) *faultInjectionNexusEndpointStore {
	return &faultInjectionNexusEndpointStore{
		NexusEndpointStore: baseStore,
		generator:          generator,
	}
}

// CreateOrUpdateNexusEndpoint wraps NexusEndpointStore.CreateOrUpdateNexusEndpoint.
func (d faultInjectionNexusEndpointStore) CreateOrUpdateNexusEndpoint(ctx context.Context, request *_sourcePersistence.InternalCreateOrUpdateNexusEndpointRequest) (err error) {
	err = d.generator.generate("CreateOrUpdateNexusEndpoint").inject(func() error {
		err = d.NexusEndpointStore.CreateOrUpdateNexusEndpoint(ctx, request)
		return err
	})
	return
}

// DeleteNexusEndpoint wraps NexusEndpointStore.DeleteNexusEndpoint.
func (d faultInjectionNexusEndpointStore) DeleteNexusEndpoint(ctx context.Context, request *_sourcePersistence.DeleteNexusEndpointRequest) (err error) {
	err = d.generator.generate("DeleteNexusEndpoint").inject(func() error {
		err = d.NexusEndpointStore.DeleteNexusEndpoint(ctx, request)
		return err
	})
	return
}

// GetNexusEndpoint wraps NexusEndpointStore.GetNexusEndpoint.
func (d faultInjectionNexusEndpointStore) GetNexusEndpoint(ctx context.Context, request *_sourcePersistence.GetNexusEndpointRequest) (ip1 *_sourcePersistence.InternalNexusEndpoint, err error) {
	err = d.generator.generate("GetNexusEndpoint").inject(func() error {
		ip1, err = d.NexusEndpointStore.GetNexusEndpoint(ctx, request)
		return err
	})
	return
}

// ListNexusEndpoints wraps NexusEndpointStore.ListNexusEndpoints.
func (d faultInjectionNexusEndpointStore) ListNexusEndpoints(ctx context.Context, request *_sourcePersistence.ListNexusEndpointsRequest) (ip1 *_sourcePersistence.InternalListNexusEndpointsResponse, err error) {
	err = d.generator.generate("ListNexusEndpoints").inject(func() error {
		ip1, err = d.NexusEndpointStore.ListNexusEndpoints(ctx, request)
		return err
	})
	return
}

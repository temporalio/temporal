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
	faultInjectionNexusEndpointStore struct {
		baseStore persistence.NexusEndpointStore
		generator faultGenerator
	}
)

func newFaultInjectionNexusEndpointStore(
	baseStore persistence.NexusEndpointStore,
	generator faultGenerator,
) *faultInjectionNexusEndpointStore {
	return &faultInjectionNexusEndpointStore{
		baseStore: baseStore,
		generator: generator,
	}
}

func (n *faultInjectionNexusEndpointStore) GetName() string {
	return n.baseStore.GetName()
}

func (n *faultInjectionNexusEndpointStore) Close() {
	n.baseStore.Close()
}

func (n *faultInjectionNexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *persistence.GetNexusEndpointRequest,
) (*persistence.InternalNexusEndpoint, error) {
	return inject1(n.generator.generate(), func() (*persistence.InternalNexusEndpoint, error) {
		return n.baseStore.GetNexusEndpoint(ctx, request)
	})
}

func (n *faultInjectionNexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *persistence.ListNexusEndpointsRequest,
) (*persistence.InternalListNexusEndpointsResponse, error) {
	return inject1(n.generator.generate(), func() (*persistence.InternalListNexusEndpointsResponse, error) {
		return n.baseStore.ListNexusEndpoints(ctx, request)
	})
}

func (n *faultInjectionNexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *persistence.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	return inject0(n.generator.generate(), func() error {
		return n.baseStore.CreateOrUpdateNexusEndpoint(ctx, request)
	})
}

func (n *faultInjectionNexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *persistence.DeleteNexusEndpointRequest,
) error {
	return inject0(n.generator.generate(), func() error {
		return n.baseStore.DeleteNexusEndpoint(ctx, request)
	})
}

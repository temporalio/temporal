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
	faultInjectionNexusIncomingServiceStore struct {
		baseStore persistence.NexusIncomingServiceStore
		generator faultGenerator
	}
)

func newFaultInjectionNexusIncomingServiceStore(
	baseStore persistence.NexusIncomingServiceStore,
	generator faultGenerator,
) *faultInjectionNexusIncomingServiceStore {
	return &faultInjectionNexusIncomingServiceStore{
		baseStore: baseStore,
		generator: generator,
	}
}

func (n *faultInjectionNexusIncomingServiceStore) GetName() string {
	return n.baseStore.GetName()
}

func (n *faultInjectionNexusIncomingServiceStore) Close() {
	n.baseStore.Close()
}

func (n *faultInjectionNexusIncomingServiceStore) GetNexusIncomingService(
	ctx context.Context,
	request *persistence.GetNexusIncomingServiceRequest,
) (*persistence.InternalNexusIncomingService, error) {
	return inject1(n.generator.generate(), func() (*persistence.InternalNexusIncomingService, error) {
		return n.baseStore.GetNexusIncomingService(ctx, request)
	})
}

func (n *faultInjectionNexusIncomingServiceStore) ListNexusIncomingServices(
	ctx context.Context,
	request *persistence.ListNexusIncomingServicesRequest,
) (*persistence.InternalListNexusIncomingServicesResponse, error) {
	return inject1(n.generator.generate(), func() (*persistence.InternalListNexusIncomingServicesResponse, error) {
		return n.baseStore.ListNexusIncomingServices(ctx, request)
	})
}

func (n *faultInjectionNexusIncomingServiceStore) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *persistence.InternalCreateOrUpdateNexusIncomingServiceRequest,
) error {
	return inject0(n.generator.generate(), func() error {
		return n.baseStore.CreateOrUpdateNexusIncomingService(ctx, request)
	})
}

func (n *faultInjectionNexusIncomingServiceStore) DeleteNexusIncomingService(
	ctx context.Context,
	request *persistence.DeleteNexusIncomingServiceRequest,
) error {
	return inject0(n.generator.generate(), func() error {
		return n.baseStore.DeleteNexusIncomingService(ctx, request)
	})
}

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
	faultInjectionMetadataStore struct {
		baseStore persistence.MetadataStore
		generator faultGenerator
	}
)

func newFaultInjectionMetadataStore(
	baseStore persistence.MetadataStore,
	generator faultGenerator,
) *faultInjectionMetadataStore {
	return &faultInjectionMetadataStore{
		baseStore: baseStore,
		generator: generator,
	}
}

func (m *faultInjectionMetadataStore) Close() {
	m.baseStore.Close()
}

func (m *faultInjectionMetadataStore) GetName() string {
	return m.baseStore.GetName()
}

func (m *faultInjectionMetadataStore) CreateNamespace(
	ctx context.Context,
	request *persistence.InternalCreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
	return inject1(m.generator.generate(), func() (*persistence.CreateNamespaceResponse, error) {
		return m.baseStore.CreateNamespace(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) GetNamespace(
	ctx context.Context,
	request *persistence.GetNamespaceRequest,
) (*persistence.InternalGetNamespaceResponse, error) {
	return inject1(m.generator.generate(), func() (*persistence.InternalGetNamespaceResponse, error) {
		return m.baseStore.GetNamespace(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) UpdateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
) error {
	return inject0(m.generator.generate(), func() error {
		return m.baseStore.UpdateNamespace(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) RenameNamespace(
	ctx context.Context,
	request *persistence.InternalRenameNamespaceRequest,
) error {
	return inject0(m.generator.generate(), func() error {
		return m.baseStore.RenameNamespace(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) DeleteNamespace(
	ctx context.Context,
	request *persistence.DeleteNamespaceRequest,
) error {
	return inject0(m.generator.generate(), func() error {
		return m.baseStore.DeleteNamespace(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *persistence.DeleteNamespaceByNameRequest,
) error {
	return inject0(m.generator.generate(), func() error {
		return m.baseStore.DeleteNamespaceByName(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) ListNamespaces(
	ctx context.Context,
	request *persistence.InternalListNamespacesRequest,
) (*persistence.InternalListNamespacesResponse, error) {
	return inject1(m.generator.generate(), func() (*persistence.InternalListNamespacesResponse, error) {
		return m.baseStore.ListNamespaces(ctx, request)
	})
}

func (m *faultInjectionMetadataStore) GetMetadata(
	ctx context.Context,
) (*persistence.GetMetadataResponse, error) {
	return inject1(m.generator.generate(), func() (*persistence.GetMetadataResponse, error) {
		return m.baseStore.GetMetadata(ctx)
	})
}

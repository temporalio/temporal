// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexustest

import (
	"context"

	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
)

// NamespaceService is a test implementation of [nexus.NamespaceService] that allows you to easily override its methods.
// It will panic if any of its methods are called without their respective On* fields being set.
type NamespaceService struct {
	OnCreateNamespace func(ctx context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error)
	OnGetNamespace    func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error)
	OnUpdateNamespace func(ctx context.Context, request *persistence.UpdateNamespaceRequest) error
}

var _ nexus.NamespaceService = (*NamespaceService)(nil)

func (n *NamespaceService) CreateNamespace(ctx context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
	return n.OnCreateNamespace(ctx, request)
}

func (n *NamespaceService) GetNamespace(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
	return n.OnGetNamespace(ctx, request)
}

func (n *NamespaceService) UpdateNamespace(ctx context.Context, request *persistence.UpdateNamespaceRequest) error {
	return n.OnUpdateNamespace(ctx, request)
}

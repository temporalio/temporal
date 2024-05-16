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

package interceptor

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
)

// gRPC method request must implement either NamespaceNameGetter or NamespaceIDGetter
// for namespace specific metrics to be reported properly
type (
	NamespaceNameGetter interface {
		GetNamespace() string
	}

	NamespaceIDGetter interface {
		GetNamespaceId() string
	}
)

// MustGetNamespaceName returns request namespace name
// or EmptyName if there's error when retriving namespace name,
// e.g. unable to find namespace
func MustGetNamespaceName(
	namespaceRegistry namespace.Registry,
	req interface{},
) namespace.Name {
	namespaceName, err := GetNamespaceName(namespaceRegistry, req)
	if err != nil {
		return namespace.EmptyName
	}
	return namespaceName
}

func GetNamespaceName(
	namespaceRegistry namespace.Registry,
	req interface{},
) (namespace.Name, error) {
	switch request := req.(type) {
	case *workflowservice.RegisterNamespaceRequest:
		// For namespace registration requests, we don't expect to find namespace so skip checking caches
		// to avoid caching a NotFound error from persistence readthrough
		return namespace.Name(request.GetNamespace()), nil
	case NamespaceNameGetter:
		namespaceName := namespace.Name(request.GetNamespace())
		_, err := namespaceRegistry.GetNamespace(namespaceName)
		if err != nil {
			return namespace.EmptyName, err
		}
		return namespaceName, nil

	case NamespaceIDGetter:
		namespaceID := namespace.ID(request.GetNamespaceId())
		namespaceName, err := namespaceRegistry.GetNamespaceName(namespaceID)
		if err != nil {
			return namespace.EmptyName, err
		}
		return namespaceName, nil

	default:
		return namespace.EmptyName, serviceerror.NewInternal(fmt.Sprintf("unable to extract namespace info from request of type %T", req))
	}
}

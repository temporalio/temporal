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

func GetNamespace(
	namespaceRegistry namespace.Registry,
	req interface{},
) namespace.Name {
	switch request := req.(type) {
	case NamespaceNameGetter:
		return namespace.Name(request.GetNamespace())

	case NamespaceIDGetter:
		namespaceID := namespace.ID(request.GetNamespaceId())
		namespaceEntry, err := namespaceRegistry.GetNamespaceByID(namespaceID)
		if err != nil {
			return ""
		}
		return namespaceEntry.Name()

	default:
		return ""
	}
}

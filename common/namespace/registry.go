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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination registry_mock.go

package namespace

import (
	"go.temporal.io/server/common/pingable"
)

type (
	// StateChangeCallbackFn can be registered to be called on any namespace state change or
	// addition/removal from database, plus once for all namespaces after registration. There
	// is no guarantee about when these are called.
	StateChangeCallbackFn func(ns *Namespace, deletedFromDb bool)

	// Registry provides access to Namespace objects by name or by ID.
	Registry interface {
		pingable.Pingable
		GetNamespace(name Name) (*Namespace, error)
		GetNamespaceWithOptions(name Name, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceByID(id ID) (*Namespace, error)
		RefreshNamespaceById(namespaceId ID) (*Namespace, error)
		GetNamespaceByIDWithOptions(id ID, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceID(name Name) (ID, error)
		GetNamespaceName(id ID) (Name, error)
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
		// Registers callback for namespace state changes.
		// StateChangeCallbackFn will be invoked for a new/deleted namespace or namespace that has
		// State, ReplicationState, ActiveCluster, or isGlobalNamespace config changed.
		RegisterStateChangeCallback(key any, cb StateChangeCallbackFn)
		UnregisterStateChangeCallback(key any)
		// GetCustomSearchAttributesMapper is a temporary solution to be able to get search attributes
		// with from persistence if forceSearchAttributesCacheRefreshOnRead is true.
		GetCustomSearchAttributesMapper(name Name) (CustomSearchAttributesMapper, error)
		Start()
		Stop()
	}

	GetNamespaceOptions struct {
		// Setting this disables the readthrough logic, i.e. only looks at the current in-memory
		// registry. This is useful if you want to avoid latency or avoid polluting the negative
		// lookup cache. Note that you may get false negatives (namespace not found) if the
		// namespace was created very recently.
		DisableReadthrough bool
	}
)

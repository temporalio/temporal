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

package chasm

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var (
	defaultShardingFn = func(key EntityKey) string { return key.NamespaceID + "_" + key.EntityID }
)

type EntityKey struct {
	NamespaceID string
	BusinessID  string
	EntityID    string
}

type ComponentRef struct {
	EntityKey

	// This is needed for routing
	rootComponentName string
	// or maybe make it specific
	// routingKey string

	shardID int32

	// From the component name, we can find the component struct definition
	// use reflection to find sub-components and understand if those sub-components
	// needs to be loaded or not. we only need to do this for sub-components of the component.
	// path for parent/ancenstor component can be inferred from the path.
	componentName      string
	componentPath      componentPath
	componentInitialVT *persistencespb.VersionedTransition // this identifies a component
	entityLastUpdateVT *persistencespb.VersionedTransition // this is consistency token

	validationFn func(Context, Component) error
}

// In V1, if you don't have a ref,
// then you can only interact with the top level entity.
func NewComponentRef(
	entityKey EntityKey,
	rootComponentName string,
) ComponentRef {
	return ComponentRef{
		EntityKey: entityKey,
		// we probably don't even need this,
		// can make the function generic and find the name from registry
		rootComponentName: rootComponentName,
	}
}

func (r *ComponentRef) Serialize() []byte {
	if r == nil {
		return nil
	}
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}

// we may need to export this later for partial loading
type componentPath []string

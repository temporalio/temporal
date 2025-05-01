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

type fieldInternal struct {
	// These 2 fields are used when node is not set yet (i.e., node==nil).
	// Don't access them directly outside of this file. Use corresponding getters instead.
	ft fieldType
	v  any // Component | Data | Pointer

	// Pointer to the corresponding tree node. Can be nil for the just created fields.
	node *Node
}

func newFieldInternalWithValue(ft fieldType, v any) fieldInternal {
	return fieldInternal{
		ft: ft,
		v:  v,
	}
}

func newFieldInternalWithNode(node *Node) fieldInternal {
	return fieldInternal{
		node: node,
	}
}

func (fi fieldInternal) isEmpty() bool {
	return fi.v == nil && fi.node == nil
}

func (fi fieldInternal) value() any {
	if fi.node == nil {
		return fi.v
	}
	return fi.node.value
}

func (fi fieldInternal) fieldType() fieldType {
	if fi.node == nil {
		return fi.ft
	}
	return fi.node.fieldType()
}

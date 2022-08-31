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

package dynamicconfig

import "go.temporal.io/server/common/log"

type (
	// StaticClient is a simple implementation of Client that just looks up in a map.
	// Values can be either plain values or []ConstrainedValue for a constrained value.
	StaticClient map[Key]any
)

func (s StaticClient) GetValue(key Key) []ConstrainedValue {
	if v, ok := s[key]; ok {
		if cvs, ok := v.([]ConstrainedValue); ok {
			return cvs
		}
		return []ConstrainedValue{{Value: v}}
	}
	return nil
}

// NewNoopClient returns a Client that has no keys (a Collection using it will always return
// default values).
func NewNoopClient() Client {
	return StaticClient(nil)
}

// NewNoopCollection creates a new noop collection.
func NewNoopCollection() *Collection {
	return NewCollection(NewNoopClient(), log.NewNoopLogger())
}

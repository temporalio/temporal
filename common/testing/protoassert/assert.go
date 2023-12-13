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

// Assert wraps testify's require package with useful helpers
package protoassert

import (
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.temporal.io/api/temporalproto"
)

type helper interface {
	Helper()
}

type ProtoAssertions struct {
	t assert.TestingT
}

func New(t assert.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

// ProtoEqual compares two proto.Message objects for equality
func ProtoEqual(t assert.TestingT, a proto.Message, b proto.Message) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff != "" {
		return assert.Fail(t, "Proto mismatch (-want +got):\n", diff)
	}
	return true
}

func NotProtoEqual(t assert.TestingT, a proto.Message, b proto.Message) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff == "" {
		return assert.Fail(t, "Expected protos to differ but they did not")
	}
	return true
}

// ProtoSliceEqual compares elements in a slice of proto.Message.
// This is not a method on the suite type because methods cannot have
// generic parameters and slice casting (say from []historyEvent) to
// []proto.Message is impossible
func ProtoSliceEqual[T proto.Message](t assert.TestingT, a []T, b []T) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if diff := cmp.Diff(a[i], b[i], protocmp.Transform()); diff != "" {
			return assert.Fail(t, "Proto mismatch at index %d (-want +got):\n%v", i, diff)
		}
	}

	return true
}

// ProtoElementsMatch behaves like assert.ElementsMatch except in that it works for
// google/protobuf-generated structs
func ProtoElementsMatch(t assert.TestingT, a any, b any, msgAndArgs ...any) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}

	if isEmpty(a) && isEmpty(b) {
		return true
	}

	extraA, extraB := diffLists(a, b)
	if len(extraA) == 0 && len(extraB) == 0 {
		return true
	}

	return assert.Fail(t, formatListDiff(a, b, extraA, extraB), msgAndArgs...)
}

func DeepEqual(t assert.TestingT, a any, b any) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}

	return temporalproto.DeepEqual(a, b)
}

func (x ProtoAssertions) ProtoEqual(a proto.Message, b proto.Message) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	return ProtoEqual(x.t, a, b)
}

func (x ProtoAssertions) NotProtoEqual(a proto.Message, b proto.Message) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	return NotProtoEqual(x.t, a, b)
}

func (x ProtoAssertions) DeepEqual(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return temporalproto.DeepEqual(a, b)
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return ProtoElementsMatch(x.t, a, b)
}

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

package protorequire

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/common/testing/protoassert"
)

type helper interface {
	Helper()
}

type ProtoAssertions struct {
	t require.TestingT
}

func New(t require.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

func ProtoEqual(t require.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoEqual(t, a, b) {
		t.FailNow()
	}
}

func NotProtoEqual(t require.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.NotProtoEqual(t, a, b) {
		t.FailNow()
	}
}

// ProtoSliceEqual compares elements in a slice of proto.Message.
// This is not a method on the suite type because methods cannot have
// generic parameters and slice casting (say from []historyEvent) to
// []proto.Message is impossible
func ProtoSliceEqual[T proto.Message](t require.TestingT, a []T, b []T) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoSliceEqual(t, a, b) {
		t.FailNow()
	}
}

func (x ProtoAssertions) ProtoEqual(a proto.Message, b proto.Message) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) NotProtoEqual(a proto.Message, b proto.Message) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.NotProtoEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) DeepEqual(a any, b any) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.DeepEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return protoassert.ProtoElementsMatch(x.t, a, b)
}

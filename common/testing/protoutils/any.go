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

package protoutils

import (
	"reflect"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func UnmarshalAny[T proto.Message](t require.TestingT, a *anypb.Any) T {
	if th, ok := t.(interface{ Helper() }); ok {
		th.Helper()
	}
	pb := new(T)
	ppb := reflect.ValueOf(pb).Elem()
	pbNew := reflect.New(reflect.TypeOf(pb).Elem().Elem())
	ppb.Set(pbNew)

	require.NoError(t, a.UnmarshalTo(*pb))
	return *pb
}

func MarshalAny(t require.TestingT, pb proto.Message) *anypb.Any {
	if th, ok := t.(interface{ Helper() }); ok {
		th.Helper()
	}
	a, err := anypb.New(pb)
	require.NoError(t, err)
	return a
}

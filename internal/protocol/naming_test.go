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

package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/server/internal/protocol"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNilSafety(t *testing.T) {
	t.Parallel()

	t.Run("nil message", func(t *testing.T) {
		pt, mt := protocol.IdentifyOrUnknown(nil)
		require.Equal(t, protocol.TypeUnknown, pt)
		require.Equal(t, protocol.MessageTypeUnknown, mt)
	})

	t.Run("nil message body", func(t *testing.T) {
		pt, mt := protocol.IdentifyOrUnknown(&protocolpb.Message{})
		require.Equal(t, protocol.TypeUnknown, pt)
		require.Equal(t, protocol.MessageTypeUnknown, mt)
	})
}

func TestWithValidMessage(t *testing.T) {
	t.Parallel()

	var empty emptypb.Empty
	var body anypb.Any
	require.NoError(t, body.MarshalFrom(&empty))

	msg := protocolpb.Message{Body: &body}

	pt, mt := protocol.IdentifyOrUnknown(&msg)

	require.Equal(t, "google.protobuf", pt.String())
	require.Equal(t, "google.protobuf.Empty", mt.String())
}

func TestWithInvalidBody(t *testing.T) {
	t.Parallel()

	var empty emptypb.Empty
	var body anypb.Any
	require.NoError(t, body.MarshalFrom(&empty))

	msg := protocolpb.Message{Body: &body}
	msg.Body.TypeUrl = "this isn't valid"

	_, _, err := protocol.Identify(&msg)
	require.Error(t, err)
}

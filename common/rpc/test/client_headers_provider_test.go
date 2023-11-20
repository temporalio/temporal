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

package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type dummyClientHeadersProvider map[string]string

func (d dummyClientHeadersProvider) GetHeaders(context.Context) (map[string]string, error) {
	return d, nil
}

func TestClientHeadersProviderInterceptor(t *testing.T) {
	clientHeadersProvider := dummyClientHeadersProvider{"a": "b", "C": "D"}
	interceptor := rpc.ClientHeadersProviderInterceptor(clientHeadersProvider)
	interceptor(context.Background(), "method", "request", "reply", nil,
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			for k, v := range clientHeadersProvider {
				require.Equal(t, 1, len(md.Get(k)))
				require.Equal(t, v, md.Get(k)[0])
			}
			return nil
		})
}

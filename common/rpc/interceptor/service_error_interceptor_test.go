// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UnaryHandler func(ctx context.Context, req any) (any, error)

type (
	// Unimplemented represents unimplemented error.
	ErrorWithoutStatus struct {
		Message string
	}
)

func (e *ErrorWithoutStatus) Error() string {
	return e.Message
}

// Error returns string message.
func TestServiceErrorInterceptorUnknown(t *testing.T) {

	_, err := ServiceErrorInterceptor(context.Background(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid argument")
		})

	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = ServiceErrorInterceptor(context.Background(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			errWithoutStatus := &ErrorWithoutStatus{
				Message: "unknown error without status",
			}
			return nil, errWithoutStatus
		})

	assert.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
}

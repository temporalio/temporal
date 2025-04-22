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

package interceptor

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaskUnknownOrInternalErrors(t *testing.T) {

	statusOk := status.New(codes.OK, "OK")
	testMaskUnknownOrInternalErrors(t, statusOk, false)

	statusUnknown := status.New(codes.Unknown, "Unknown")
	testMaskUnknownOrInternalErrors(t, statusUnknown, true)

	statusInternal := status.New(codes.Internal, "Internal")
	testMaskUnknownOrInternalErrors(t, statusInternal, true)
}

func testMaskUnknownOrInternalErrors(t *testing.T, st *status.Status, expectRelpace bool) {
	controller := gomock.NewController(t)
	mockRegistry := namespace.NewMockRegistry(controller)
	mockLogger := log.NewMockLogger(controller)
	dc := dynamicconfig.NewNoopCollection()
	errorMaskInterceptor := NewMaskInternalErrorDetailsInterceptor(
		dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), mockRegistry, mockLogger)

	err := serviceerror.FromStatus(st)
	if expectRelpace {
		mockLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)
	}
	errorMessage := errorMaskInterceptor.maskUnknownOrInternalErrors(nil, "test", "", err)
	if expectRelpace {
		errorHash := common.ErrorHash(err)
		expectedMessage := fmt.Sprintf("rpc error: code = %s desc = %s (%s)", st.Message(), errorFrontendMasked, errorHash)

		assert.Equal(t, expectedMessage, errorMessage.Error())
	} else {
		if err == nil {
			assert.Equal(t, errorMessage, nil)
		} else {
			assert.Equal(t, errorMessage.Error(), st.Message())
		}
	}
}

func TestMaskInternalErrorDetailsInterceptor(t *testing.T) {

	controller := gomock.NewController(t)
	mockRegistry := namespace.NewMockRegistry(controller)
	mockLogger := log.NewMockLogger(controller)

	// Create a mock error for testing
	testError := status.Error(codes.Internal, "test error")

	// Create a mock handler that always returns the test error
	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, testError
	}

	// Test cases for different namespace scenarios
	testCases := []struct {
		name              string
		ctx               context.Context
		maskInternalError bool
		expectMaskedError bool
	}{
		{
			name:              "Non-empty namespace with maskInternalError=false",
			ctx:               headers.SetCallerInfo(context.Background(), headers.NewCallerInfo("test-namespace", "", "")),
			maskInternalError: false,
			expectMaskedError: false, // Should NOT mask errors when maskInternalError=false
		},
		{
			name:              "Non-empty namespace with maskInternalError=true",
			ctx:               headers.SetCallerInfo(context.Background(), headers.NewCallerInfo("test-namespace-2", "", "")),
			maskInternalError: true,
			expectMaskedError: true, // Should mask errors when maskInternalError=true
		},
		{
			name:              "Empty namespace",
			ctx:               headers.SetCallerInfo(context.Background(), headers.NewCallerInfo("", "", "")),
			maskInternalError: false,
			expectMaskedError: false, // Should NOT mask errors for empty namespace
		},
		{
			name:              "No caller info",
			ctx:               context.Background(), // Empty context without caller info
			maskInternalError: false,
			expectMaskedError: false, // Should NOT mask errors when no caller info
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a custom maskInternalError function based on the test case
			maskInternalErrorFn := func(namespace string) bool {
				return tc.maskInternalError
			}

			// Create the interceptor with the custom maskInternalError function
			errorMask := NewMaskInternalErrorDetailsInterceptor(
				maskInternalErrorFn, mockRegistry, mockLogger)

			if tc.expectMaskedError {
				mockLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)
			}

			// Call Intercept directly
			_, err := errorMask.Intercept(tc.ctx, nil, &grpc.UnaryServerInfo{FullMethod: "test-method"}, mockHandler)

			// Verify the error is masked or not based on expectations
			if tc.expectMaskedError {
				assert.Contains(t, err.Error(), errorFrontendMasked)
			} else {
				// If an error should not be masked, it should be the original error
				assert.Equal(t, testError, err)
			}
		})
	}
}

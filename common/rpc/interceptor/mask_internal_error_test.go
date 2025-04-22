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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// shouldMaskErrors is a helper method for testing
func (mi *MaskInternalErrorDetailsInterceptor) shouldMaskErrors(ctx context.Context) bool {
	nsName := headers.GetCallerInfo(ctx).CallerName
	if nsName == "" {
		return false
	}
	return mi.maskInternalError(nsName)
}

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
	dc := dynamicconfig.NewNoopCollection()
	mockLogger := log.NewMockLogger(controller)

	errorMask := NewMaskInternalErrorDetailsInterceptor(
		dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), mockRegistry, mockLogger)

	test_namespace := "test-namespace"
	ctx := headers.SetCallerInfo(context.Background(), headers.NewCallerInfo(test_namespace, "", ""))
	assert.True(t, errorMask.shouldMaskErrors(ctx))

	namespace_not_found := "namespace-not-found"
	ctx = headers.SetCallerInfo(context.Background(), headers.NewCallerInfo(namespace_not_found, "", ""))
	assert.True(t, errorMask.shouldMaskErrors(ctx))

	empty_namespace := ""
	ctx = headers.SetCallerInfo(context.Background(), headers.NewCallerInfo(empty_namespace, "", ""))
	assert.False(t, errorMask.shouldMaskErrors(ctx))

	// Test with empty context (no caller info)
	emptyCtx := context.Background()
	assert.False(t, errorMask.shouldMaskErrors(emptyCtx))
}

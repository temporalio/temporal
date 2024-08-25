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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
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
	err := serviceerror.FromStatus(st)
	errorMessage := maskUnknownOrInternalErrors(err)
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

	mockRegistry := namespace.NewMockRegistry(gomock.NewController(t))
	dc := dynamicconfig.NewNoopCollection()
	errorMask := NewMaskInternalErrorDetailsInterceptor(dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), mockRegistry)

	test_namespace := "test-namespace"
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: test_namespace}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(test_namespace)).Return(&namespace.Namespace{}, nil).AnyTimes()
	assert.True(t, errorMask.shouldMaskErrors(req))

	namespace_not_found := "namespace-not-found"
	req = &workflowservice.StartWorkflowExecutionRequest{Namespace: namespace_not_found}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(namespace_not_found)).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	assert.False(t, errorMask.shouldMaskErrors(req))

	empty_namespace := ""
	req = &workflowservice.StartWorkflowExecutionRequest{Namespace: empty_namespace}
	mockRegistry.EXPECT().GetNamespace(namespace.Name(empty_namespace)).Return(nil, serviceerror.NewNamespaceNotFound("missing-namespace"))
	assert.False(t, errorMask.shouldMaskErrors(req))

	var ei interface{}
	assert.False(t, errorMask.shouldMaskErrors(ei))
}

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

	"go.temporal.io/server/common/namespace"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MaskInternalErrorsInterceptor struct {
	maskInternalError dynamicconfig.BoolPropertyFnWithNamespaceFilter
	namespaceRegistry namespace.Registry
}

func NewMaskInternalErrorsInterceptor(
	dc *dynamicconfig.Collection,
	namespaceRegistry namespace.Registry,
) *MaskInternalErrorsInterceptor {

	return &MaskInternalErrorsInterceptor{
		maskInternalError: dynamicconfig.MaskInternalOrUnknownErrors.Get(dc),
		namespaceRegistry: namespaceRegistry,
	}
}

func (i *MaskInternalErrorsInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	resp, err := handler(ctx, req)
	if err != nil && i.shouldMaskErrors(req) {
		err = maskUnknownOrInternalErrors(err)
	}
	return resp, err
}

func (i *MaskInternalErrorsInterceptor) shouldMaskErrors(req interface{}) bool {
	ns := MustGetNamespaceName(i.namespaceRegistry, req)
	if ns.IsEmpty() {
		return false
	}
	return i.maskInternalError(ns.String())
}

var errorFrontendMasked = "something went wrong, please retry"

func maskUnknownOrInternalErrors(err error) error {
	st := serviceerror.ToStatus(err)
	if st.Code() != codes.Unknown && st.Code() != codes.Internal {
		return err
	}

	// convert internal and unknown errors into neutral error with hash code of the original error
	errorHash := common.ErrorHash(err)
	maskedErrorMessage := fmt.Sprintf("%s (%s)", errorFrontendMasked, errorHash)
	return status.New(st.Code(), maskedErrorMessage).Err()
}

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

package gocql

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/persistence"
)

func ConvertError(
	operation string,
	err error,
) error {

	switch err {
	case nil:
		return nil
	case context.DeadlineExceeded, gocql.ErrTimeoutNoResponse, gocql.ErrConnectionClosed:
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, err.Error())}
	case gocql.ErrNotFound:
		return serviceerror.NewNotFound(fmt.Sprintf("operation %v encountered %v", operation, err.Error()))
	}

	switch v := err.(type) {
	case *gocql.RequestErrWriteTimeout:
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, err.Error())}
	case gocql.RequestError:
		if v.Code() == gocql.ErrCodeOverloaded {
			return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				fmt.Sprintf("operation %v encountered %v", operation, err.Error()))
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("operation %v encountered %v", operation, err.Error()))
	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("operation %v encountered %v", operation, err.Error()))
	}
}

func IsNotFoundError(err error) bool {
	return err == gocql.ErrNotFound
}

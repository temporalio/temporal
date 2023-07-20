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

package store

import (
	"strings"

	"go.temporal.io/api/serviceerror"
)

type (
	VisibilityStoreInvalidValuesError struct {
		errs []error
	}
)

var (
	// OperationNotSupportedErr is returned when visibility operation in not supported.
	OperationNotSupportedErr = serviceerror.NewInvalidArgument("Operation not supported. Please use on Elasticsearch")
)

func (e *VisibilityStoreInvalidValuesError) Error() string {
	var sb strings.Builder
	sb.WriteString("Visibility store invalid values errors: ")
	for _, err := range e.errs {
		sb.WriteString("[")
		sb.WriteString(err.Error())
		sb.WriteString("]")
	}
	return sb.String()
}

func NewVisibilityStoreInvalidValuesError(errs []error) error {
	return &VisibilityStoreInvalidValuesError{errs: errs}
}

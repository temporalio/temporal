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

package matcher

import (
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
)

type (
	Error struct {
		message string
	}
)

var (
	malformedSqlQueryErrMessage = "malformed SQL query"
	notSupportedErrMessage      = "operation is not supported"
	invalidExpressionErrMessage = "invalid expression"
)

func NewMatcherError(format string, a ...interface{}) error {
	message := fmt.Sprintf(format, a...)
	return &Error{message: message}
}

func (c *Error) Error() string {
	return c.message
}

func (c *Error) ToInvalidArgument() error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid query: %v", c))
}

func wrapMatcherError(message string, err error) error {
	var matcherErr *Error
	if errors.As(err, &matcherErr) {
		return NewMatcherError("%s: %v", message, matcherErr)
	}
	return err
}

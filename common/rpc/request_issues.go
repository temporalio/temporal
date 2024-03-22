// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestIssues is a collection of request validation issues for an RPC. The zero value is valid to use.
type RequestIssues struct {
	issues []string
}

// Append an issue to the set.
func (ri *RequestIssues) Append(issue string) {
	ri.issues = append(ri.issues, issue)
}

// Appendf appends a formatted issue to the set.
func (ri *RequestIssues) Appendf(format string, args ...interface{}) {
	ri.Append(fmt.Sprintf(format, args...))
}

// GetError returns a gRPC INVALID_ARGUMENT error representing the issues in the set or nil if there are no issues.
func (ri *RequestIssues) GetError() error {
	if len(ri.issues) == 0 {
		return nil
	}
	return status.Errorf(codes.InvalidArgument, strings.Join(ri.issues, ", "))
}

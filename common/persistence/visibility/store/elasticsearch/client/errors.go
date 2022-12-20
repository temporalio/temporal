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

package client

import (
	"github.com/olivere/elastic/v7"
)

var (
	// retryItemStatusCodes is an array of status codes that indicate that a bulk
	// response line item should be retried. Should match default value from
	// elastic.defaultRetryItemStatusCodes.
	// 408 - Request Timeout
	// 429 - Too Many Requests
	// 503 - Service Unavailable
	// 507 - Insufficient Storage
	retryItemStatusCodes = []int{408, 429, 503, 507}
)

// IsRetryableStatus check if httpsStatus is retryable.
func IsRetryableStatus(httpStatus int) bool {
	for _, code := range retryItemStatusCodes {
		if httpStatus == code {
			return true
		}
	}
	return false
}

func HttpStatus(err error) int {
	switch e := err.(type) {
	case *elastic.Error:
		return e.Status
	default:
		return 0
	}
}

func IsRetryableError(err error) bool {
	return IsRetryableStatus(HttpStatus(err))
}

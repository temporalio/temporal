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

package elasticsearch

import (
	"fmt"
	"time"
)

type (
	VisibilityTaskNAckError struct {
		VisibilityTaskKey string
	}

	VisibilityTaskAckTimeoutError struct {
		VisibilityTaskKey string
		Timeout           time.Duration
	}
)

func newVisibilityTaskNAckError(visibilityTaskKey string) error {
	return &VisibilityTaskNAckError{
		VisibilityTaskKey: visibilityTaskKey,
	}
}

func (v *VisibilityTaskNAckError) Error() string {
	return fmt.Sprintf("visibility task %s wasn't acknowledged", v.VisibilityTaskKey)
}

func newVisibilityTaskAckTimeoutError(visibilityTaskKey string, timeout time.Duration) error {
	return &VisibilityTaskAckTimeoutError{
		VisibilityTaskKey: visibilityTaskKey,
		Timeout:           timeout,
	}
}

func (v *VisibilityTaskAckTimeoutError) Error() string {
	return fmt.Sprintf("visibility task %s acknowledge timedout after %v", v.VisibilityTaskKey, v.Timeout)
}

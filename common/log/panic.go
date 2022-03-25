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

package log

import (
	"fmt"
	"runtime/debug"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log/tag"
)

// CapturePanic is used to capture panic, it will log the panic and also return the error through pointer.
// If the panic value is not error then a default error is returned
// We have to use pointer is because in golang: "recover return nil if was not called directly by a deferred function."
// And we have to set the returned error otherwise our handler will return nil as error which is incorrect
func CapturePanic(logger Logger, retError *error) {
	if panicObj := recover(); panicObj != nil {
		err, ok := panicObj.(error)
		if !ok {
			err = fmt.Errorf("panic: %v", panicObj)
		}

		st := string(debug.Stack())

		logger.Error("Panic is captured", tag.SysStackTrace(st), tag.Error(err))

		*retError = serviceerror.NewInternal(err.Error())
	}
}

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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package log

import (
	"go.temporal.io/server/common/log/tag"
)

type (
	// Logger is our abstraction for logging
	// Usage examples:
	//  import "go.temporal.io/server/common/log/tag"
	//  1) logger = log.With(
	//          logger,
	//          tag.WorkflowNextEventID(123),
	//          tag.WorkflowActionWorkflowStarted,
	//          tag.WorkflowNamespaceID("test-namespace-id"))
	//     logger.Info("hello world")
	//  2) logger.Info("hello world",
	//          tag.WorkflowNextEventID(123),
	//          tag.WorkflowActionWorkflowStarted,
	//          tag.WorkflowNamespaceID("test-namespace-id"))
	//	   )
	//  Note: msg should be static, it is not recommended to use fmt.Sprintf() for msg.
	//        Anything dynamic should be tagged.
	Logger interface {
		Debug(msg string, tags ...tag.Tag)
		Info(msg string, tags ...tag.Tag)
		Warn(msg string, tags ...tag.Tag)
		Error(msg string, tags ...tag.Tag)
		Fatal(msg string, tags ...tag.Tag)
	}

	// Implement WithLogger to efficiently create a logger with added tags.
	WithLogger interface {
		With(tags ...tag.Tag) Logger
	}

	// Implement SkipLogger to efficiently create a logger which skips extraSkip frames from call stack.
	SkipLogger interface {
		Skip(extraSkip int) Logger
	}
)

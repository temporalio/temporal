// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/uber/cadence/common/log/tag"
)

// Logger is our abstraction for logging
// Usage examples:
//  import "github.com/uber/cadence/common/log/tag"
//  1) logger = logger.WithTags(
//          tag.WorkflowNextEventID( 123),
//          tag.WorkflowActionWorkflowStarted,
//          tag.WorkflowDomainID("test-domain-id"))
//     logger.Info("hello world")
//  2) logger.Info("hello world",
//          tag.WorkflowNextEventID( 123),
//          tag.WorkflowActionWorkflowStarted,
//          tag.WorkflowDomainID("test-domain-id"))
//	   )
//  Note: msg should be static, it is not recommended to use fmt.Sprintf() for msg.
//        Anything dynamic should be tagged.
type Logger interface {
	Debug(msg string, tags ...tag.Tag)
	Info(msg string, tags ...tag.Tag)
	Warn(msg string, tags ...tag.Tag)
	Error(msg string, tags ...tag.Tag)
	Fatal(msg string, tags ...tag.Tag)
	WithTags(tags ...tag.Tag) Logger
}

type noop struct{}

// NewNoop return a noop logger
func NewNoop() Logger {
	return &noop{}
}

func (n *noop) Debug(msg string, tags ...tag.Tag) {}
func (n *noop) Info(msg string, tags ...tag.Tag)  {}
func (n *noop) Warn(msg string, tags ...tag.Tag)  {}
func (n *noop) Error(msg string, tags ...tag.Tag) {}
func (n *noop) Fatal(msg string, tags ...tag.Tag) {}
func (n *noop) WithTags(tags ...tag.Tag) Logger {
	return n
}

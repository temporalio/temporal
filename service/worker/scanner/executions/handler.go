// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import "github.com/uber/cadence/service/worker/scanner/executor"

type handlerStatus = executor.TaskStatus

const (
	handlerStatusDone  = executor.TaskStatusDone
	handlerStatusErr   = executor.TaskStatusErr
	handlerStatusDefer = executor.TaskStatusDefer
)

const scannerTaskListPrefix = "cadence-sys-executions-scanner"

// validateHandler validates a single execution.
// It operates in two phases: collection step and validation step.
// During collection step information from persistence is read for this workflow execution.
// During validation step invariants are asserted over everything that was read.
// In the future its possible to add a third step here which will additionally take automatic recovery actions if validation failed.
func (s *Scavenger) validateHandler(key *executionKey) handlerStatus {
	// TODO: implement this
	return handlerStatusDone
}

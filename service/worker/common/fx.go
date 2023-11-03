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

package common

import "go.uber.org/fx"

// WorkerComponentTag is the fx group tag for worker components. This is used to allow those who use Temporal as a
// library to dynamically register their own system workers. Use this to annotate a worker component consumer. Use the
// AnnotateWorkerComponentProvider function to annotate a worker component provider.
const WorkerComponentTag = `group:"workerComponent"`

// AnnotateWorkerComponentProvider converts a WorkerComponent factory function into an fx provider which will add the
// WorkerComponentTag to the result.
func AnnotateWorkerComponentProvider[T any](f func(t T) WorkerComponent) fx.Option {
	return fx.Provide(fx.Annotate(f, fx.ResultTags(WorkerComponentTag)))
}

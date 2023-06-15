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

package temporal

import (
	"time"

	"go.temporal.io/server/common/primitives"
)

const (
	mismatchLogMessage  = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
	serviceStartTimeout = time.Duration(15) * time.Second
	serviceStopTimeout  = time.Duration(5) * time.Minute
)

type (
	Server interface {
		Start() error
		Stop() error
	}
)

var (
	// Services is the set of all valid temporal services as strings (needs to be strings to
	// keep ServerOptions interface stable)
	Services = []string{
		string(primitives.FrontendService),
		string(primitives.InternalFrontendService),
		string(primitives.HistoryService),
		string(primitives.MatchingService),
		string(primitives.WorkerService),
	}

	// DefaultServices is the set of services to start by default if services are not given on
	// the command line.
	DefaultServices = []string{
		string(primitives.FrontendService),
		string(primitives.HistoryService),
		string(primitives.MatchingService),
		string(primitives.WorkerService),
	}
)

// NewServer returns a new instance of server that serves one or many services.
func NewServer(opts ...ServerOption) (Server, error) {
	return NewServerFx(TopLevelModule, opts...)
}

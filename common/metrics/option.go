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

package metrics

type (
	// Option adds information to a metric definition.
	// Consumers of Option values may do nothing with them,
	// i.e. they may be specific to the metrics handler implementation.
	Option interface {
		apply(p *params)
	}

	// params are the set of parameters that can be provided to functions which return a new metric.
	params struct {
		// optionalHelpText is the name of the help text that will be set in the help tag.
		// If this is nil, no help text will be added.
		optionalHelpText *string
	}
)

// buildParams takes a set of options and constructs the params used to define a metric.
func buildParams(opts []Option) params {
	p := params{
		optionalHelpText: nil,
	}
	for _, opt := range opts {
		opt.apply(&p)
	}

	return p
}

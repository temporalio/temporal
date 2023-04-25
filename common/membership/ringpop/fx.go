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

package ringpop

import (
	"go.uber.org/fx"

	"go.temporal.io/server/common/membership"
)

// Module provides a membership.Monitor given the types in factoryParams. It also adds lifecycle hooks, so there's no
// need to start or stop the monitor manually.
var Module = fx.Provide(
	provideMonitor,
)

func provideMonitor(lc fx.Lifecycle, params factoryParams) (membership.Monitor, error) {
	f, err := newFactory(params)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.StopHook(f.closeTChannel))

	m, err := f.getMonitor()
	if err != nil {
		return nil, err
	}

	lc.Append(fx.StartStopHook(m.Start, m.Stop))

	return m, nil
}

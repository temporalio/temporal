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

// MembershipModule provides membership objects given the types in factoryParams.
var MembershipModule = fx.Provide(
	provideFactory,
	provideMembership,
	provideHostInfoProvider,
)

func provideFactory(lc fx.Lifecycle, params factoryParams) (*factory, error) {
	f, err := newFactory(params)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.StopHook(f.closeTChannel))
	return f, nil
}

func provideMembership(lc fx.Lifecycle, f *factory) membership.Monitor {
	m := f.getMonitor()
	lc.Append(fx.StopHook(m.Stop))
	return m
}

func provideHostInfoProvider(lc fx.Lifecycle, f *factory) (membership.HostInfoProvider, error) {
	return f.getHostInfoProvider()
}

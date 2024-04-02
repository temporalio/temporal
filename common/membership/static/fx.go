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

package static

import (
	"fmt"

	"go.uber.org/fx"

	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

func MembershipModule(
	hostsByService map[primitives.ServiceName]Hosts,
) fx.Option {
	return fx.Options(
		fx.Provide(func() membership.Monitor {
			return newStaticMonitor(hostsByService)
		}),
		fx.Provide(func(serviceName primitives.ServiceName) membership.HostInfoProvider {
			hosts := hostsByService[serviceName]
			if len(hosts.All) == 0 {
				panic(fmt.Sprintf("hosts for %v service are missing in static hosts", serviceName))
			}
			if len(hosts.Self) == 0 {
				panic(fmt.Sprintf("self host for %v service is missing in static hosts", serviceName))
			}

			for _, serviceHost := range hosts.All {
				if serviceHost == hosts.Self {
					hostInfo := membership.NewHostInfoFromAddress(serviceHost)
					return membership.NewHostInfoProvider(hostInfo)
				}
			}
			panic(fmt.Sprintf("self host %v for %v service is defined, but missing from the list of all static hosts", hosts.Self, serviceName))
		}),
	)
}

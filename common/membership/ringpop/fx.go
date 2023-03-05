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
	"context"

	"go.uber.org/fx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
)

var Module = fx.Options(
	fx.Provide(membershipMonitorProvider),
)

func membershipMonitorProvider(
	lc fx.Lifecycle,
	clusterMetadataManager persistence.ClusterMetadataManager,
	logger log.SnTaggedLogger,
	cfg *config.Config,
	svcName primitives.ServiceName,
	tlsConfigProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
) (membership.Monitor, error) {
	servicePortMap := make(map[primitives.ServiceName]int)
	for sn, sc := range cfg.Services {
		servicePortMap[primitives.ServiceName(sn)] = sc.RPC.GRPCPort
	}

	rpcConfig := cfg.Services[string(svcName)].RPC

	f, err := newFactory(
		&cfg.Global.Membership,
		svcName,
		servicePortMap,
		logger,
		clusterMetadataManager,
		&rpcConfig,
		tlsConfigProvider,
		dc,
	)
	if err != nil {
		return nil, err
	}

	m, err := f.getMonitor()
	if err != nil {
		return nil, err
	}

	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				m.Start()
				return nil
			},
			OnStop: func(context.Context) error {
				m.Stop()
				f.closeTChannel()
				return nil
			},
		},
	)

	return m, nil
}

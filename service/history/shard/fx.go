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

package shard

import (
	"context"
	"sync/atomic"

	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/configs"
)

var Module = fx.Options(
	fx.Provide(
		ControllerProvider,
		func(impl *ControllerImpl) Controller { return impl },
		ContextFactoryProvider,
		fx.Annotate(
			func(p Controller) common.Pingable { return p },
			fx.ResultTags(`group:"deadlockDetectorRoots"`),
		),
	),
	ownershipBasedQuotaScalerModule,
)

var ownershipBasedQuotaScalerModule = fx.Options(
	fx.Provide(func(
		impl *ControllerImpl,
		cfg *configs.Config,
	) (*OwnershipBasedQuotaScalerImpl, error) {
		return NewOwnershipBasedQuotaScaler(
			impl,
			int(cfg.NumberOfShards),
			nil,
		)
	}),
	fx.Provide(func(
		impl *OwnershipBasedQuotaScalerImpl,
	) OwnershipBasedQuotaScaler {
		return impl
	}),
	fx.Provide(func() LazyLoadedOwnershipBasedQuotaScaler {
		return LazyLoadedOwnershipBasedQuotaScaler{
			Value: &atomic.Value{},
		}
	}),
	fx.Invoke(initLazyLoadedOwnershipBasedQuotaScaler),
	fx.Invoke(func(
		lc fx.Lifecycle,
		impl *OwnershipBasedQuotaScalerImpl,
	) {
		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				impl.Close()
				return nil
			},
		})
	}),
)

func initLazyLoadedOwnershipBasedQuotaScaler(
	serviceName primitives.ServiceName,
	logger log.SnTaggedLogger,
	ownershipBasedQuotaScaler OwnershipBasedQuotaScaler,
	lazyLoadedOwnershipBasedQuotaScaler LazyLoadedOwnershipBasedQuotaScaler,
) {
	lazyLoadedOwnershipBasedQuotaScaler.Store(ownershipBasedQuotaScaler)
	logger.Info("Initialized lazy loaded OwnershipBasedQuotaScaler", tag.Service(serviceName))
}

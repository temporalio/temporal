// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"github.com/google/wire"
	"github.com/urfave/cli/v2"
	tlog "go.temporal.io/server/common/log"
	"go.uber.org/dig"
)

var UserSet = wire.NewSet(
	DefaultConfigProvider,
	DefaultLogger,
	wire.Bind(new(NamespaceLogger), new(tlog.Logger)),
	DefaultDynamicConfigClientProvider,
	DefaultAuthorizerProvider,
	DefaultClaimMapper,
	DefaultServiceNameListProvider,
	DefaultDatastoreFactory,
	DefaultMetricsReportersProvider,
	DefaultTLSConfigProvider,
	DefaultDynamicConfigCollectionProvider,
	DefaultAudienceGetterProvider,
	DefaultPersistenseServiceResolverProvider,
	DefaultElasticSearchHttpClientProvider,
	DefaultInterruptChProvider,
)

var serverSet = wire.NewSet(
	ServicesProvider,
	ServerProvider,
	AdvancedVisibilityStoreProvider,
	ESClientProvider,
	ESConfigProvider,
	AdvancedVisibilityWritingModeProvider,
	wire.Struct(new(ServicesProviderDeps), "*"), // we can use this to inject dependencies into sub-modules
)

func InjectServerProviders(dc *dig.Container, c *cli.Context) error {
	if err := dc.Provide(func() *cli.Context { return c }); err != nil {
		return err
	}
	if err := dc.Provide(DefaultConfigProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultLogger); err != nil {
		return err
	}
	if err := dc.Provide(func(l tlog.Logger) NamespaceLogger { return l }); err != nil {
		return err
	}
	if err := dc.Provide(DefaultDynamicConfigClientProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultAuthorizerProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultClaimMapper); err != nil {
		return err
	}
	if err := dc.Provide(DefaultServiceNameListProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultDatastoreFactory); err != nil {
		return err
	}
	if err := dc.Provide(DefaultMetricsReportersProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultTLSConfigProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultDynamicConfigCollectionProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultAudienceGetterProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultPersistenseServiceResolverProvider); err != nil {
		return err
	}
	if err := dc.Provide(DefaultElasticSearchHttpClientProvider); err != nil {
		return err
	}

	// todomigryz: we need to check for err on each dc.Provide
	if err := dc.Provide(DefaultInterruptChProvider); err != nil {
		return err
	}
	if err := dc.Provide(ServicesProvider); err != nil {
		return err
	}
	if err := dc.Provide(ServerProvider); err != nil {
		return err
	}
	if err := dc.Provide(AdvancedVisibilityStoreProvider); err != nil {
		return err
	}
	if err := dc.Provide(ESClientProvider); err != nil {
		return err
	}
	if err := dc.Provide(ESConfigProvider); err != nil {
		return err
	}
	if err := dc.Provide(AdvancedVisibilityWritingModeProvider); err != nil {
		return err
	}
	// if err := dc.Provide(MatchingServiceProvider); err != nil {
	// 	return err
	// }
	return nil
}

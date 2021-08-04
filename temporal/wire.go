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

//+build wireinject

package temporal

import (
	"github.com/google/wire"
	"github.com/urfave/cli/v2"
)

func InitializeDefaultUserProviderSet(c *cli.Context) wire.ProviderSet {
	return wire.NewSet(
		DefaultConfigProvider,
		DefaultLoggerProvider,
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
	)
}

var UserSet = wire.NewSet(
	DefaultConfigProvider,
	DefaultLoggerProvider,
	DefaultNamespaceLoggerProvider,
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
	wire.Struct(new(ProviderCommonParams), "*"), // we can use this to inject dependencies into sub-modules
)

func InitializeServer(c *cli.Context) (*Server, error) {
	wire.Build(
		UserSet,
		serverSet,
	)
	return nil, nil
}

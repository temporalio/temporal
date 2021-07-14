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

func InitializeServerContainer(c *cli.Context) (*dig.Container, error) {
	dc := dig.New()
	dc.Provide(func () *cli.Context {return c})
	dc.Provide(DefaultConfigProvider)
	dc.Provide(DefaultConfigProvider)
	dc.Provide(DefaultLogger)
	dc.Provide(func (l tlog.Logger) NamespaceLogger { return l })
	dc.Provide(DefaultDynamicConfigClientProvider)
	dc.Provide(DefaultAuthorizerProvider)
	dc.Provide(DefaultClaimMapper)
	dc.Provide(DefaultServiceNameListProvider)
	dc.Provide(DefaultDatastoreFactory)
	dc.Provide(DefaultMetricsReportersProvider)
	dc.Provide(DefaultTLSConfigProvider)
	dc.Provide(DefaultDynamicConfigCollectionProvider)
	dc.Provide(DefaultAudienceGetterProvider)
	dc.Provide(DefaultPersistenseServiceResolverProvider)
	dc.Provide(DefaultElasticSearchHttpClientProvider)
	err := dc.Provide(DefaultInterruptChProvider)
	if err != nil {
		println(err.Error())
	} else {
		println("All good")
	}
	err = dc.Provide(ServicesProvider)
	if err != nil {
		println(err.Error())
	} else {
		println("All good")
	}
	dc.Provide(ServerProvider)
	dc.Provide(AdvancedVisibilityStoreProvider)
	dc.Provide(ESClientProvider)
	dc.Provide(ESConfigProvider)
	dc.Provide(AdvancedVisibilityWritingModeProvider)
	// println(dc.String())
	// dig.Visualize(dc, os.Stdout)
	return dc, nil
}

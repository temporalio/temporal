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
	"github.com/urfave/cli/v2"
	tlog "go.temporal.io/server/common/log"
	"go.uber.org/dig"
)

const (
	UserConfigProvider                     = "UserConfigProvider"
	UserLoggerProvider                     = "UserLoggerProvider"
	UserNamespaceLoggerProvider            = "UserNamespaceLoggerProvider"
	UserDynamicConfigClientProvider        = "UserDynamicConfigClientProvider"
	UserAuthorizerProvider                 = "UserAuthorizerProvider"
	UserClaimMapperProvider                = "UserClaimMapperProvider"
	UserServiceNameListProvider            = "UserServiceNameListProvider"
	UserDatastoreFactoryProvider           = "UserDatastoreFactoryProvider"
	UserMetricsReportersProvider           = "UserMetricsReportersProvider"
	UserTLSConfigProvider                  = "UserTLSConfigProvider"
	UserDynamicConfigCollectionProvider    = "UserDynamicConfigCollectionProvider"
	UserAudienceGetterProvider             = "UserAudienceGetterProvider"
	UserPersistenceServiceResolverProvider = "UserPersistenceServiceResolverProvider"
	UserElasticSearchHttpClientProvider    = "UserElasticSearchHttpClientProvider"
	UserInterruptChProvider                = "UserInterruptChProvider"
)

func InjectServerProviders(dc *dig.Container, c *cli.Context, customProviders map[string]interface{}) error {
	for _, val := range customProviders {
		if err := dc.Provide(val); err != nil {
			return err
		}
	}

	if err := dc.Provide(func() *cli.Context { return c }); err != nil {
		return err
	}

	if _, ok := customProviders[UserConfigProvider]; !ok {
		if err := dc.Provide(DefaultConfigProvider); err != nil {
			return err
		}
	}

	if _, ok := customProviders[UserLoggerProvider]; !ok {
		if err := dc.Provide(DefaultLogger); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserNamespaceLoggerProvider]; !ok {
		if err := dc.Provide(func(l tlog.Logger) NamespaceLogger { return l }); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserDynamicConfigClientProvider]; !ok {
		if err := dc.Provide(DefaultDynamicConfigClientProvider); err != nil {
			return err
		}
	}

	if _, ok := customProviders[UserAuthorizerProvider]; !ok {
		if err := dc.Provide(DefaultAuthorizerProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserClaimMapperProvider]; !ok {
		if err := dc.Provide(DefaultClaimMapper); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserServiceNameListProvider]; !ok {
		if err := dc.Provide(DefaultServiceNameListProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserDatastoreFactoryProvider]; !ok {
		if err := dc.Provide(DefaultDatastoreFactory); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserMetricsReportersProvider]; !ok {
		if err := dc.Provide(DefaultMetricsReportersProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserTLSConfigProvider]; !ok {
		if err := dc.Provide(DefaultTLSConfigProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserDynamicConfigCollectionProvider]; !ok {
		if err := dc.Provide(DefaultDynamicConfigCollectionProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserAudienceGetterProvider]; !ok {
		if err := dc.Provide(DefaultAudienceGetterProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserPersistenceServiceResolverProvider]; !ok {
		if err := dc.Provide(DefaultPersistenceServiceResolverProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserElasticSearchHttpClientProvider]; !ok {
		if err := dc.Provide(DefaultElasticSearchHttpClientProvider); err != nil {
			return err
		}
	}
	if _, ok := customProviders[UserInterruptChProvider]; !ok {
		if err := dc.Provide(DefaultInterruptChProvider); err != nil {
			return err
		}
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

	return nil
}

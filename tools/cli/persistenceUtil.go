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

package cli

import (
	"fmt"

	"github.com/urfave/cli"

	"github.com/temporalio/temporal/common/auth"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

// InitializePersistenceFactory returns an initialized persistence managers factory.
// The factory allows to easily initialize concrete persistence managers to execute commands against persistence layer
func InitializePersistenceFactory(c *cli.Context) persistenceClient.Factory {

	defaultStore, err := InitializeDefaultDBConfig(c)

	if err != nil {
		ErrorAndExit("InitializePersistenceFactory err", err)
	}

	visibilityStore, err := InitializeVisibilityDBConfig(c)

	if err != nil {
		ErrorAndExit("InitializePersistenceFactory err", err)
	}

	persistence := config.Persistence{
		DefaultStore:    "db-default",
		VisibilityStore: "db-visibility",
		DataStores: map[string]config.DataStore{
			"db-default":    defaultStore,
			"db-visibility": visibilityStore,
		},
	}

	params := resource.BootstrapParams{}
	params.Name = "cli"

	factory := persistenceClient.NewFactory(
		&persistence,
		GetQPS,
		params.AbstractDatastoreFactory,
		c.String(FlagTargetCluster),
		nil, // MetricsClient
		nil,
	)

	return factory
}

// InitializeDefaultDBConfig return default DB configuration based on provided options
func InitializeDefaultDBConfig(c *cli.Context) (config.DataStore, error) {

	engine := getRequiredOption(c, FlagDBEngine)

	if engine == "cassandra" {
		defaultConfig := &config.Cassandra{
			Hosts:    getRequiredOption(c, FlagDBAddress),
			Port:     c.Int(FlagDBPort),
			User:     c.String(FlagUsername),
			Password: c.String(FlagPassword),
			Keyspace: getRequiredOption(c, FlagKeyspace),
		}

		if c.Bool(FlagEnableTLS) {
			defaultConfig.TLS = &auth.TLS{
				Enabled:                true,
				CertFile:               c.String(FlagTLSCertPath),
				KeyFile:                c.String(FlagTLSKeyPath),
				CaFile:                 c.String(FlagTLSCaPath),
				EnableHostVerification: c.Bool(FlagTLSEnableHostVerification),
			}
		}

		defaultStore := config.DataStore{
			Cassandra: defaultConfig,
		}

		return defaultStore, nil
	}

	return config.DataStore{}, fmt.Errorf("DB type is not supported by CLI")
}

// InitializeVisibilityDBConfig return visibility DB configuration based on provided options
func InitializeVisibilityDBConfig(c *cli.Context) (config.DataStore, error) {

	engine := getRequiredOption(c, FlagDBEngine)

	if engine == "cassandra" {
		defaultConfig := &config.Cassandra{
			Hosts:    getRequiredOption(c, FlagDBAddress),
			Port:     c.Int(FlagDBPort),
			User:     c.String(FlagUsername),
			Password: c.String(FlagPassword),
			Keyspace: "temporal_visibility", //getRequiredOption(c, FlagKeyspace), TODO
		}

		if c.Bool(FlagEnableTLS) {
			defaultConfig.TLS = &auth.TLS{
				Enabled:                true,
				CertFile:               c.String(FlagTLSCertPath),
				KeyFile:                c.String(FlagTLSKeyPath),
				CaFile:                 c.String(FlagTLSCaPath),
				EnableHostVerification: c.Bool(FlagTLSEnableHostVerification),
			}
		}

		defaultStore := config.DataStore{
			Cassandra: defaultConfig,
		}

		return defaultStore, nil
	}

	return config.DataStore{}, fmt.Errorf("DB type is not supported by CLI")
}

// GetQPS returns default queries per second
func GetQPS(...dynamicconfig.FilterOption) int {
	return 3000
}

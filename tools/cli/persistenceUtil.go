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

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
)

// CreatePersistenceFactory returns an initialized persistence managers factory.
// The factory allows to easily initialize concrete persistence managers to execute commands against persistence layer
func CreatePersistenceFactory(c *cli.Context) persistenceClient.Factory {
	defaultStore, err := CreateDefaultDBConfig(c)
	if err != nil {
		ErrorAndExit("CreatePersistenceFactory err", err)
	}

	visibilityStore, _ := CreateDefaultDBConfig(c)
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
		resolver.NewNoopResolver(),
		GetQPS,
		nil,
		c.String(FlagTargetCluster),
		nil, // MetricsClient
		log.NewNoopLogger(),
	)

	return factory
}

// CreateDefaultDBConfig return default DB configuration based on provided options
func CreateDefaultDBConfig(c *cli.Context) (config.DataStore, error) {
	engine := getRequiredOption(c, FlagDBEngine)

	var tls *auth.TLS
	if c.Bool(FlagEnableTLS) {
		tls = &auth.TLS{
			Enabled:                true,
			CertFile:               c.String(FlagTLSCertPath),
			KeyFile:                c.String(FlagTLSKeyPath),
			CaFile:                 c.String(FlagTLSCaPath),
			ServerName:             c.String(FlagTLSServerName),
			EnableHostVerification: !c.Bool(FlagTLSDisableHostVerification),
		}
	}

	var defaultStore config.DataStore

	switch engine {
	case cassandraDBType:
		defaultConfig := &config.Cassandra{
			Hosts:    c.String(FlagDBAddress),
			Port:     c.Int(FlagDBPort),
			User:     c.String(FlagUsername),
			Password: c.String(FlagPassword),
			Keyspace: c.String(FlagKeyspace),
			TLS:      tls,
		}
		defaultStore.Cassandra = defaultConfig
	case mysql.PluginName, postgresql.PluginName:
		addr := fmt.Sprintf("%v:%v", c.String(FlagDBAddress), c.Int(FlagDBPort))
		defaultConfig := &config.SQL{
			User:         c.String(FlagUsername),
			Password:     c.String(FlagPassword),
			DatabaseName: c.String(FlagKeyspace),
			ConnectAddr:  addr,
			PluginName:   engine,
			TLS:          tls,
		}

		defaultStore.SQL = defaultConfig
	default:
		return config.DataStore{}, fmt.Errorf("DB type %q is not supported by CLI", engine)
	}
	return defaultStore, nil
}

// GetQPS returns default queries per second
func GetQPS(...dynamicconfig.FilterOption) int {
	return 3000
}

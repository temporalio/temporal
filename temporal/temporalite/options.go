// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporalite

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/temporal"
)

// WithLogger overrides the default logger.
func WithLogger(logger log.Logger) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.Logger = logger
	})
}

// WithDatabaseFilePath persists state to the file at the specified path.
func WithDatabaseFilePath(filepath string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.Ephemeral = false
		cfg.DatabaseFilePath = filepath
	})
}

// WithPersistenceDisabled disables file persistence and uses the in-memory storage driver.
// State will be reset on each process restart.
func WithPersistenceDisabled() ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.Ephemeral = true
	})
}

// WithFrontendPort sets the listening port for the temporal-frontend GRPC service.
//
// When unspecified, the default port number of 7233 is used.
func WithFrontendPort(port int) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.FrontendPort = port
	})
}

// WithMetricsPort sets the listening port for metrics.
//
// When unspecified, the port will be system-chosen.
func WithMetricsPort(port int) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.MetricsPort = port
	})
}

// WithFrontendIP binds the temporal-frontend GRPC service to a specific IP (eg. `0.0.0.0`)
// Check net.ParseIP for supported syntax; only IPv4 is supported.
//
// When unspecified, the frontend service will bind to localhost.
func WithFrontendIP(address string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.FrontendIP = address
	})
}

// WithDynamicPorts starts Temporal on system-chosen ports.
func WithDynamicPorts() ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.DynamicPorts = true
	})
}

// WithNamespaces registers each namespace on Temporal start.
func WithNamespaces(namespaces ...string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.Namespaces = append(cfg.Namespaces, namespaces...)
	})
}

// WithSQLitePragmas applies pragma statements to SQLite on Temporal start.
func WithSQLitePragmas(pragmas map[string]string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		if cfg.SQLitePragmas == nil {
			cfg.SQLitePragmas = make(map[string]string)
		}
		for k, v := range pragmas {
			cfg.SQLitePragmas[k] = v
		}
	})
}

// WithOptions registers Temporal server options.
func WithOptions(options ...temporal.ServerOption) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.ServerOptions = append(cfg.ServerOptions, options...)
	})
}

// WithBaseConfig sets the default Temporal server configuration.
//
// Storage and client configuration will always be overridden, however base config can be
// used to enable settings like TLS or authentication.
func WithBaseConfig(base *config.Config) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		cfg.BaseConfig = base
	})
}

// WithDynamicConfigValue sets the given dynamic config key with the given set
// of values. This will overwrite a key if already set.
func WithDynamicConfigValue(key dynamicconfig.Key, value []dynamicconfig.ConstrainedValue) ServerOption {
	return newApplyFuncContainer(func(cfg *liteConfig) {
		if cfg.DynamicConfig == nil {
			cfg.DynamicConfig = dynamicconfig.StaticClient{}
		}
		cfg.DynamicConfig[key] = value
	})
}

// WithSearchAttributeCacheDisabled forces refreshing the search attributes cache during each read operation.
//
// This effectively bypasses any cached value and is used to facilitate testing of changes in search attributes.
// This should not be turned on in production.
func WithSearchAttributeCacheDisabled() ServerOption {
	return WithDynamicConfigValue(
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead,
		[]dynamicconfig.ConstrainedValue{{Value: true}},
	)
}

type applyFuncContainer struct {
	applyInternal func(*liteConfig)
}

func (fso *applyFuncContainer) apply(cfg *liteConfig) {
	fso.applyInternal(cfg)
}

func newApplyFuncContainer(apply func(*liteConfig)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}

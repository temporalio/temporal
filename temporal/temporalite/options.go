// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporalite

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/temporal/internal/liteconfig"
)

// WithLogger overrides the default logger.
func WithLogger(logger log.Logger) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.Logger = logger
	})
}

// WithDatabaseFilePath persists state to the file at the specified path.
func WithDatabaseFilePath(filepath string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.Ephemeral = false
		cfg.DatabaseFilePath = filepath
	})
}

// WithPersistenceDisabled disables file persistence and uses the in-memory storage driver.
// State will be reset on each process restart.
func WithPersistenceDisabled() ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.Ephemeral = true
	})
}

// UIServer abstracts the github.com/temporalio/ui-server project to
// make it an optional import for programs that need web UI support.
//
// A working implementation of this interface is available here:
// https://pkg.go.dev/github.com/temporalio/ui-server/server#Server
type UIServer interface {
	Start() error
	Stop()
}

// WithUI enables the Temporal web interface.
//
// When unspecified, Temporal will run in headless mode.
//
// This option accepts a UIServer implementation in order to avoid bloating
// programs that do not need to embed the UI.
// See ./cmd/temporalite/main.go for an example of usage.
func WithUI(server UIServer) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.UIServer = server
	})
}

// WithFrontendPort sets the listening port for the temporal-frontend GRPC service.
//
// When unspecified, the default port number of 7233 is used.
func WithFrontendPort(port int) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.FrontendPort = port
	})
}

// WithMetricsPort sets the listening port for metrics.
//
// When unspecified, the port will be system-chosen.
func WithMetricsPort(port int) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.MetricsPort = port
	})
}

// WithFrontendIP binds the temporal-frontend GRPC service to a specific IP (eg. `0.0.0.0`)
// Check net.ParseIP for supported syntax; only IPv4 is supported.
//
// When unspecified, the frontend service will bind to localhost.
func WithFrontendIP(address string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.FrontendIP = address
	})
}

// WithDynamicPorts starts Temporal on system-chosen ports.
func WithDynamicPorts() ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.DynamicPorts = true
	})
}

// WithNamespaces registers each namespace on Temporal start.
func WithNamespaces(namespaces ...string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.Namespaces = append(cfg.Namespaces, namespaces...)
	})
}

// WithSQLitePragmas applies pragma statements to SQLite on Temporal start.
func WithSQLitePragmas(pragmas map[string]string) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
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
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.ServerOptions = append(cfg.ServerOptions, options...)
	})
}

// WithBaseConfig sets the default Temporal server configuration.
//
// Storage and client configuration will always be overridden, however base config can be
// used to enable settings like TLS or authentication.
func WithBaseConfig(base *config.Config) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
		cfg.BaseConfig = base
	})
}

// WithDynamicConfigValue sets the given dynamic config key with the given set
// of values. This will overwrite a key if already set.
func WithDynamicConfigValue(key dynamicconfig.Key, value []dynamicconfig.ConstrainedValue) ServerOption {
	return newApplyFuncContainer(func(cfg *liteconfig.Config) {
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
	applyInternal func(*liteconfig.Config)
}

func (fso *applyFuncContainer) apply(cfg *liteconfig.Config) {
	fso.applyInternal(cfg)
}

func newApplyFuncContainer(apply func(*liteconfig.Config)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}

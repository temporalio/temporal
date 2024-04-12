// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

// Package temporalite contains high level helpers for setting up a SQLite based server.
package temporalite

// TODO(jlegrone): Refactor this package into one or more temporal.ServerOption types.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	sqliteplugin "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/schema/sqlite"
	"go.temporal.io/server/temporal"
)

const localBroadcastAddress = "127.0.0.1"

// LiteServerConfig encodes options for LiteServer instances.
type LiteServerConfig struct {
	// When true, Ephemeral disables file persistence and uses the in-memory storage driver.
	// State will be reset on each process restart.
	Ephemeral bool
	// DatabaseFilePath persists state to the file at the specified path. If the db file does
	// not already exist, it is created and schema migrations are automatically run.
	//
	// This is required if Ephemeral is false.
	DatabaseFilePath string
	// Address on which frontend service should listen.
	FrontendIP string
	// Port on which frontend service should listen.
	FrontendPort int
	// WithMetricsPort sets the listening port for the default Prometheus metrics handler.
	//
	// When unspecified, the port will be system-chosen.
	//
	// This field is ignored when the WithCustomMetricsHandler server option is enabled.
	MetricsPort int
	// Namespaces specified here will be automatically registered on Temporal start.
	Namespaces []string
	// SQLitePragmas specified here will be applied as pragma statements to SQLite on Temporal start.
	SQLitePragmas map[string]string
	// Logger overrides the default logger.
	Logger log.Logger
	// BaseConfig sets the default Temporal server configuration.
	//
	// Storage and client configuration will always be overridden, however base config can be
	// used to enable settings like TLS or authentication.
	//
	// Note that ServerOption arguments can also be passed to the NewLiteServer function.
	// Always prefer setting BaseConfig over using WithConfig however, as WithConfig overrides
	// all LiteServer specific settings.
	BaseConfig *config.Config
	// DynamicConfig sets dynamic config values used by the server.
	DynamicConfig dynamicconfig.StaticClient
}

func (cfg *LiteServerConfig) apply(serverConfig *config.Config, provider *PortProvider) {
	sqliteConfig := config.SQL{
		PluginName:        sqliteplugin.PluginName,
		ConnectAttributes: make(map[string]string),
		DatabaseName:      cfg.DatabaseFilePath,
	}
	if cfg.Ephemeral {
		sqliteConfig.ConnectAttributes["mode"] = "memory"
		sqliteConfig.ConnectAttributes["cache"] = "shared"
		// TODO(jlegrone): investigate whether a randomized db name is necessary when running in shared cache mode:
		//                 https://www.sqlite.org/sharedcache.html
		sqliteConfig.DatabaseName = fmt.Sprintf("%d", rand.Intn(9999999))
	} else {
		sqliteConfig.ConnectAttributes["mode"] = "rwc"
	}

	for k, v := range cfg.SQLitePragmas {
		sqliteConfig.ConnectAttributes["_"+k] = v
	}

	if cfg.FrontendPort == 0 {
		cfg.FrontendPort = provider.MustGetFreePort()
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = provider.MustGetFreePort()
	}
	pprofPort := provider.MustGetFreePort()

	serverConfig.Global.Membership = config.Membership{
		MaxJoinDuration:  30 * time.Second,
		BroadcastAddress: localBroadcastAddress,
	}
	serverConfig.Global.Metrics = &metrics.Config{
		Prometheus: &metrics.PrometheusConfig{
			ListenAddress: fmt.Sprintf("%s:%d", cfg.FrontendIP, cfg.MetricsPort),
			HandlerPath:   "/metrics",
		},
	}
	serverConfig.Global.PProf = config.PProf{Port: pprofPort}
	serverConfig.Persistence = config.Persistence{
		DefaultStore:     sqliteplugin.PluginName,
		VisibilityStore:  sqliteplugin.PluginName,
		NumHistoryShards: 1,
		DataStores: map[string]config.DataStore{
			sqliteplugin.PluginName: {SQL: &sqliteConfig},
		},
	}
	serverConfig.ClusterMetadata = &cluster.Config{
		EnableGlobalNamespace:    false,
		FailoverVersionIncrement: 10,
		MasterClusterName:        "active",
		CurrentClusterName:       "active",
		ClusterInformation: map[string]cluster.ClusterInformation{
			"active": {
				Enabled:                true,
				InitialFailoverVersion: 1,
				RPCAddress:             fmt.Sprintf("%s:%d", localBroadcastAddress, cfg.FrontendPort),
			},
		},
	}
	serverConfig.DCRedirectionPolicy = config.DCRedirectionPolicy{
		Policy: "noop",
	}
	serverConfig.Services = map[string]config.Service{
		"frontend": cfg.mustGetService(0, provider),
		"history":  cfg.mustGetService(1, provider),
		"matching": cfg.mustGetService(2, provider),
		"worker":   cfg.mustGetService(3, provider),
	}
	serverConfig.Archival = config.Archival{
		History: config.HistoryArchival{
			State:      "disabled",
			EnableRead: false,
			Provider:   nil,
		},
		Visibility: config.VisibilityArchival{
			State:      "disabled",
			EnableRead: false,
			Provider:   nil,
		},
	}
	// TODO(dnr): Figure out why server fails to start when PublicClient is not set with error:
	//            panic: Client must be created with client.Dial() or client.NewLazyClient()
	//            See also: https://github.com/temporalio/temporal/pull/4026#discussion_r1149808018
	serverConfig.PublicClient = config.PublicClient{
		HostPort: fmt.Sprintf("%s:%d", localBroadcastAddress, cfg.FrontendPort),
	}
	serverConfig.NamespaceDefaults = config.NamespaceDefaults{
		Archival: config.ArchivalNamespaceDefaults{
			History: config.HistoryArchivalNamespaceDefaults{
				State: "disabled",
			},
			Visibility: config.VisibilityArchivalNamespaceDefaults{
				State: "disabled",
			},
		},
	}
}

func (cfg *LiteServerConfig) applyDefaults() {
	if cfg.BaseConfig == nil {
		cfg.BaseConfig = &config.Config{}
	}
	if cfg.Logger == nil {
		cfg.Logger = log.NewZapLogger(log.BuildZapLogger(log.Config{
			Stdout:     true,
			Level:      "info",
			OutputFile: "",
		}))
	}
}

func (cfg *LiteServerConfig) validate() error {
	for pragma := range cfg.SQLitePragmas {
		if _, ok := supportedPragmas[strings.ToLower(pragma)]; !ok {
			return fmt.Errorf("unsupported SQLite pragma %q. allowed pragmas: %v", pragma, getAllowedPragmas())
		}
	}

	if cfg.Ephemeral && cfg.DatabaseFilePath != "" {
		return fmt.Errorf("config option DatabaseFilePath is not supported in ephemeral mode")
	}
	if !cfg.Ephemeral && cfg.DatabaseFilePath == "" {
		return fmt.Errorf("config option DatabaseFilePath is required when ephemeral mode disabled")
	}

	return nil
}

// LiteServer is a high level wrapper for Server that automatically configures a SQLite backend.
type LiteServer struct {
	internal         temporal.Server
	frontendHostPort string
}

// NewLiteServer initializes a Server with a SQLite backend.
//
// Additional configuration can be specified either via variadic ServerOption arguments or
// by setting the BaseConfig field in LiteServerConfig.
//
// Always use BaseConfig instead of the WithConfig server option, as WithConfig overrides all
// LiteServer specific settings.
func NewLiteServer(liteConfig *LiteServerConfig, opts ...temporal.ServerOption) (*LiteServer, error) {
	liteConfig.applyDefaults()
	if err := liteConfig.validate(); err != nil {
		return nil, err
	}

	p := NewPortProvider()
	liteConfig.apply(liteConfig.BaseConfig, p)
	if err := p.Close(); err != nil {
		return nil, err
	}

	sqlConfig := liteConfig.BaseConfig.Persistence.DataStores[sqliteplugin.PluginName].SQL

	if !liteConfig.Ephemeral {
		// Apply migrations if file does not already exist
		if _, err := os.Stat(liteConfig.DatabaseFilePath); os.IsNotExist(err) {
			// Check if any of the parent dirs are missing
			dir := filepath.Dir(liteConfig.DatabaseFilePath)
			if _, err := os.Stat(dir); err != nil {
				return nil, fmt.Errorf("error setting up schema: %w", err)
			}

			if err := sqlite.SetupSchema(sqlConfig); err != nil {
				return nil, fmt.Errorf("error setting up schema: %w", err)
			}
		}
	}

	// Pre-create namespaces
	var namespaces []*sqlite.NamespaceConfig
	for _, ns := range liteConfig.Namespaces {
		namespaces = append(namespaces, sqlite.NewNamespaceConfig(liteConfig.BaseConfig.ClusterMetadata.CurrentClusterName, ns, false))
	}
	if err := sqlite.CreateNamespaces(sqlConfig, namespaces...); err != nil {
		return nil, fmt.Errorf("error creating namespaces: %w", err)
	}

	authorizer, err := authorization.GetAuthorizerFromConfig(&liteConfig.BaseConfig.Global.Authorization)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate authorizer: %w", err)
	}

	claimMapper, err := authorization.GetClaimMapperFromConfig(&liteConfig.BaseConfig.Global.Authorization, liteConfig.Logger)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate claim mapper: %w", err)
	}

	serverOpts := []temporal.ServerOption{
		temporal.WithConfig(liteConfig.BaseConfig),
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithLogger(liteConfig.Logger),
		temporal.WithAuthorizer(authorizer),
		temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
			return claimMapper
		}),
	}

	if len(liteConfig.DynamicConfig) > 0 {
		// To prevent having to code fall-through semantics right now, we currently
		// eagerly fail if dynamic config is being configured in two ways
		if liteConfig.BaseConfig.DynamicConfigClient != nil {
			return nil, fmt.Errorf("unable to have file-based dynamic config and individual dynamic config values")
		}
		serverOpts = append(serverOpts, temporal.WithDynamicConfigClient(liteConfig.DynamicConfig))
	}

	// Apply options from arguments
	serverOpts = append(serverOpts, opts...)

	srv, err := temporal.NewServer(serverOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate server: %w", err)
	}

	s := &LiteServer{
		internal:         srv,
		frontendHostPort: liteConfig.BaseConfig.PublicClient.HostPort,
	}

	return s, nil
}

// Start temporal server.
func (s *LiteServer) Start() error {
	// We wrap Server instead of simply embedding it in the LiteServer struct so
	// that it's possible to add additional lifecycle hooks here if necessary.
	return s.internal.Start()
}

// Stop the server.
func (s *LiteServer) Stop() error {
	// We wrap Server instead of simply embedding it in the LiteServer struct so
	// that it's possible to add additional lifecycle hooks here if necessary.
	return s.internal.Stop()
}

// NewClient initializes a client ready to communicate with the Temporal
// server in the target namespace.
func (s *LiteServer) NewClient(ctx context.Context, namespace string) (client.Client, error) {
	return s.NewClientWithOptions(ctx, client.Options{Namespace: namespace})
}

// NewClientWithOptions is the same as NewClient but allows further customization.
//
// To set the client's namespace, use the corresponding field in client.Options.
//
// Note that options.HostPort will always be overridden.
func (s *LiteServer) NewClientWithOptions(ctx context.Context, options client.Options) (client.Client, error) {
	options.HostPort = s.frontendHostPort
	return client.Dial(options)
}

// FrontendHostPort returns the host:port for this server.
//
// When constructing a Temporalite client from within the same process,
// NewClient or NewClientWithOptions should be used instead.
func (s *LiteServer) FrontendHostPort() string {
	return s.frontendHostPort
}

var supportedPragmas = map[string]struct{}{
	"journal_mode": {},
	"synchronous":  {},
}

func getAllowedPragmas() []string {
	allowedPragmaList := maps.Keys(supportedPragmas)
	sort.Strings(allowedPragmaList)
	return allowedPragmaList
}

func (cfg *LiteServerConfig) mustGetService(frontendPortOffset int, provider *PortProvider) config.Service {
	svc := config.Service{
		RPC: config.RPC{
			GRPCPort:        cfg.FrontendPort + frontendPortOffset,
			MembershipPort:  provider.MustGetFreePort(),
			BindOnLocalHost: true,
			BindOnIP:        "",
		},
	}

	// Assign any open port when configured to use dynamic ports
	if frontendPortOffset != 0 {
		svc.RPC.GRPCPort = provider.MustGetFreePort()
	}

	// Optionally bind frontend to IPv4 address
	if frontendPortOffset == 0 && cfg.FrontendIP != "" {
		svc.RPC.BindOnLocalHost = false
		svc.RPC.BindOnIP = cfg.FrontendIP
	}

	return svc
}

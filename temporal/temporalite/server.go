// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporalite

import (
	"os"

	"context"
	"fmt"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/client"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	sqliteplugin "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/schema/sqlite"
	"go.temporal.io/server/temporal"
)

// TemporaliteServer is a high level wrapper for temporal.Server that automatically configures a sqlite backend.
type TemporaliteServer struct {
	internal         temporal.Server
	frontendHostPort string
	config           *liteConfig
}

type ServerOption interface {
	apply(*liteConfig)
}

// NewServer returns a TemporaliteServer with a sqlite backend.
func NewServer(opts ...ServerOption) (*TemporaliteServer, error) {
	c, err := newDefaultConfig()
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.apply(c)
	}

	for pragma := range c.SQLitePragmas {
		if _, ok := supportedPragmas[strings.ToLower(pragma)]; !ok {
			return nil, fmt.Errorf("ERROR: unsupported pragma %q, %v allowed", pragma, getAllowedPragmas())
		}
	}

	cfg := convertLiteConfig(c)
	sqlConfig := cfg.Persistence.DataStores[sqliteplugin.PluginName].SQL

	if !c.Ephemeral {
		// Apply migrations if file does not already exist
		if _, err := os.Stat(c.DatabaseFilePath); os.IsNotExist(err) {
			// Check if any of the parent dirs are missing
			dir := filepath.Dir(c.DatabaseFilePath)
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
	for _, ns := range c.Namespaces {
		namespaces = append(namespaces, sqlite.NewNamespaceConfig(cfg.ClusterMetadata.CurrentClusterName, ns, false))
	}
	if err := sqlite.CreateNamespaces(sqlConfig, namespaces...); err != nil {
		return nil, fmt.Errorf("error creating namespaces: %w", err)
	}

	authorizer, err := authorization.GetAuthorizerFromConfig(&cfg.Global.Authorization)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate authorizer: %w", err)
	}

	claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, c.Logger)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate claim mapper: %w", err)
	}

	serverOpts := []temporal.ServerOption{
		temporal.WithConfig(cfg),
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithLogger(c.Logger),
		temporal.WithAuthorizer(authorizer),
		temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
			return claimMapper
		}),
	}

	if len(c.DynamicConfig) > 0 {
		// To prevent having to code fall-through semantics right now, we currently
		// eagerly fail if dynamic config is being configured in two ways
		if cfg.DynamicConfigClient != nil {
			return nil, fmt.Errorf("unable to have file-based dynamic config and individual dynamic config values")
		}
		serverOpts = append(serverOpts, temporal.WithDynamicConfigClient(c.DynamicConfig))
	}

	if len(c.ServerOptions) > 0 {
		serverOpts = append(serverOpts, c.ServerOptions...)
	}

	srv, err := temporal.NewServer(serverOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate server: %w", err)
	}

	s := &TemporaliteServer{
		internal:         srv,
		frontendHostPort: cfg.PublicClient.HostPort,
		config:           c,
	}

	return s, nil
}

// Start temporal server.
func (s *TemporaliteServer) Start() error {
	return s.internal.Start()
}

// Stop the server.
func (s *TemporaliteServer) Stop() {
	if s == nil {
		return
	}
	s.internal.Stop()
}

// NewClient initializes a client ready to communicate with the Temporal
// server in the target namespace.
func (s *TemporaliteServer) NewClient(ctx context.Context, namespace string) (client.Client, error) {
	return s.NewClientWithOptions(ctx, client.Options{Namespace: namespace})
}

// NewClientWithOptions is the same as NewClient but allows further customization.
//
// To set the client's namespace, use the corresponding field in client.Options.
//
// Note that the HostPort and ConnectionOptions fields of client.Options will always be overridden.
func (s *TemporaliteServer) NewClientWithOptions(ctx context.Context, options client.Options) (client.Client, error) {
	options.HostPort = s.frontendHostPort
	return client.NewClient(options)
}

// FrontendHostPort returns the host:port for this server.
//
// When constructing a Temporalite client from within the same process,
// NewClient or NewClientWithOptions should be used instead.
func (s *TemporaliteServer) FrontendHostPort() string {
	return s.frontendHostPort
}

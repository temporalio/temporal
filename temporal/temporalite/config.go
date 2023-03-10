// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporalite

import (
	"math/rand"
	"os"
	"time"

	"fmt"
	"path/filepath"
	"sort"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

const (
	broadcastAddress    = "127.0.0.1"
	defaultFrontendPort = 7233
)

var supportedPragmas = map[string]struct{}{
	"journal_mode": {},
	"synchronous":  {},
}

func getAllowedPragmas() []string {
	var allowedPragmaList []string
	for k := range supportedPragmas {
		allowedPragmaList = append(allowedPragmaList, k)
	}
	sort.Strings(allowedPragmaList)
	return allowedPragmaList
}

func newDefaultConfig() (*serverConfig, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("cannot determine user config directory: %w", err)
	}

	return &serverConfig{
		Ephemeral:        false,
		DatabaseFilePath: filepath.Join(userConfigDir, "temporalite", "db", "default.db"),
		FrontendPort:     0,
		MetricsPort:      0,
		DynamicPorts:     false,
		Namespaces:       nil,
		SQLitePragmas:    nil,
		Logger: log.NewZapLogger(log.BuildZapLogger(log.Config{
			Stdout:     true,
			Level:      "info",
			OutputFile: "",
		})),
		portProvider: newPortProvider(),
		FrontendIP:   "",
		BaseConfig:   &config.Config{},
	}, nil
}

func convertLiteConfig(cfg *serverConfig) *config.Config {
	defer func() {
		if err := cfg.portProvider.Close(); err != nil {
			panic(err)
		}
	}()

	sqliteConfig := config.SQL{
		PluginName:        sqlite.PluginName,
		ConnectAttributes: make(map[string]string),
		DatabaseName:      cfg.DatabaseFilePath,
	}
	if cfg.Ephemeral {
		sqliteConfig.ConnectAttributes["mode"] = "memory"
		sqliteConfig.ConnectAttributes["cache"] = "shared"
		sqliteConfig.DatabaseName = fmt.Sprintf("%d", rand.Intn(9999999))
	} else {
		sqliteConfig.ConnectAttributes["mode"] = "rwc"
	}

	for k, v := range cfg.SQLitePragmas {
		sqliteConfig.ConnectAttributes["_"+k] = v
	}

	var pprofPort int
	if cfg.DynamicPorts {
		if cfg.FrontendPort == 0 {
			cfg.FrontendPort = cfg.portProvider.MustGetFreePort()
		}
		if cfg.MetricsPort == 0 {
			cfg.MetricsPort = cfg.portProvider.MustGetFreePort()
		}
		pprofPort = cfg.portProvider.MustGetFreePort()
	} else {
		if cfg.FrontendPort == 0 {
			cfg.FrontendPort = defaultFrontendPort
		}
		if cfg.MetricsPort == 0 {
			cfg.MetricsPort = cfg.FrontendPort + 200
		}
		pprofPort = cfg.FrontendPort + 201
	}

	baseConfig := cfg.BaseConfig
	baseConfig.Global.Membership = config.Membership{
		MaxJoinDuration:  30 * time.Second,
		BroadcastAddress: broadcastAddress,
	}
	baseConfig.Global.Metrics = &metrics.Config{
		Prometheus: &metrics.PrometheusConfig{
			ListenAddress: fmt.Sprintf("%s:%d", cfg.FrontendIP, cfg.MetricsPort),
			HandlerPath:   "/metrics",
		},
	}
	baseConfig.Global.PProf = config.PProf{Port: pprofPort}
	baseConfig.Persistence = config.Persistence{
		DefaultStore:     sqlite.PluginName,
		VisibilityStore:  sqlite.PluginName,
		NumHistoryShards: 1,
		DataStores: map[string]config.DataStore{
			sqlite.PluginName: {SQL: &sqliteConfig},
		},
	}
	baseConfig.ClusterMetadata = &cluster.Config{
		EnableGlobalNamespace:    false,
		FailoverVersionIncrement: 10,
		MasterClusterName:        "active",
		CurrentClusterName:       "active",
		ClusterInformation: map[string]cluster.ClusterInformation{
			"active": {
				Enabled:                true,
				InitialFailoverVersion: 1,
				RPCAddress:             fmt.Sprintf("%s:%d", broadcastAddress, cfg.FrontendPort),
			},
		},
	}
	baseConfig.DCRedirectionPolicy = config.DCRedirectionPolicy{
		Policy: "noop",
	}
	baseConfig.Services = map[string]config.Service{
		"frontend": cfg.mustGetService(0),
		"history":  cfg.mustGetService(1),
		"matching": cfg.mustGetService(2),
		"worker":   cfg.mustGetService(3),
	}
	baseConfig.Archival = config.Archival{
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
	baseConfig.PublicClient = config.PublicClient{
		HostPort: fmt.Sprintf("%s:%d", broadcastAddress, cfg.FrontendPort),
	}
	baseConfig.NamespaceDefaults = config.NamespaceDefaults{
		Archival: config.ArchivalNamespaceDefaults{
			History: config.HistoryArchivalNamespaceDefaults{
				State: "disabled",
			},
			Visibility: config.VisibilityArchivalNamespaceDefaults{
				State: "disabled",
			},
		},
	}

	return baseConfig
}

func (cfg *serverConfig) mustGetService(frontendPortOffset int) config.Service {
	svc := config.Service{
		RPC: config.RPC{
			GRPCPort:        cfg.FrontendPort + frontendPortOffset,
			MembershipPort:  cfg.FrontendPort + 100 + frontendPortOffset,
			BindOnLocalHost: true,
			BindOnIP:        "",
		},
	}

	// Assign any open port when configured to use dynamic ports
	if cfg.DynamicPorts {
		if frontendPortOffset != 0 {
			svc.RPC.GRPCPort = cfg.portProvider.MustGetFreePort()
		}
		svc.RPC.MembershipPort = cfg.portProvider.MustGetFreePort()
	}

	// Optionally bind frontend to IPv4 address
	if frontendPortOffset == 0 && cfg.FrontendIP != "" {
		svc.RPC.BindOnLocalHost = false
		svc.RPC.BindOnIP = cfg.FrontendIP
	}

	return svc
}

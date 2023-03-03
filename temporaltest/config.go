// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Copyright (c) 2021 Datadog, Inc.
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

package temporaltest

import (
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

const (
	broadcastAddress     = "127.0.0.1"
	PersistenceStoreName = "sqlite-default"
	DefaultFrontendPort  = 7233
	DefaultMetricsPort   = 0
)

func newDefaultConfig() (*config.Config, error) {
	cfg := &config.Config{}

	pp := NewPortProvider()
	defer func() {
		if err := pp.Close(); err != nil {
			panic(err)
		}
	}()

	sqliteConfig := config.SQL{
		PluginName:   sqlite.PluginName,
		DatabaseName: fmt.Sprintf("%d", rand.Intn(9999999)),
		ConnectAttributes: map[string]string{
			"mode":  "memory",
			"cache": "shared",
		},
	}

	frontendPort := pp.MustGetFreePort()
	metricsPort := pp.MustGetFreePort()
	pprofPort := pp.MustGetFreePort()

	cfg.Global.Membership = config.Membership{
		MaxJoinDuration:  30 * time.Second,
		BroadcastAddress: broadcastAddress,
	}
	cfg.Global.Metrics = &metrics.Config{
		Prometheus: &metrics.PrometheusConfig{
			ListenAddress: fmt.Sprintf("%s:%d", "127.0.0.1", metricsPort),
			HandlerPath:   "/metrics",
		},
	}
	cfg.Global.PProf = config.PProf{Port: pprofPort}
	cfg.Persistence = config.Persistence{
		DefaultStore:     PersistenceStoreName,
		VisibilityStore:  PersistenceStoreName,
		NumHistoryShards: 1,
		DataStores: map[string]config.DataStore{
			PersistenceStoreName: {SQL: &sqliteConfig},
		},
	}

	cfg.ClusterMetadata = &cluster.Config{
		EnableGlobalNamespace:    false,
		FailoverVersionIncrement: 10,
		MasterClusterName:        "active",
		CurrentClusterName:       "active",
		ClusterInformation: map[string]cluster.ClusterInformation{
			"active": {
				Enabled:                true,
				InitialFailoverVersion: 1,
				RPCAddress:             fmt.Sprintf("%s:%d", broadcastAddress, frontendPort),
			},
		},
	}

	cfg.DCRedirectionPolicy = config.DCRedirectionPolicy{
		Policy: "noop",
	}

	cfg.Services = map[string]config.Service{
		"frontend": mustGetService(pp, frontendPort),
		"history":  mustGetService(pp, 0),
		"matching": mustGetService(pp, 0),
		"worker":   mustGetService(pp, 0),
	}

	cfg.Archival = config.Archival{
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

	cfg.PublicClient = config.PublicClient{
		HostPort: fmt.Sprintf("%s:%d", broadcastAddress, frontendPort),
	}

	cfg.NamespaceDefaults = config.NamespaceDefaults{
		Archival: config.ArchivalNamespaceDefaults{
			History: config.HistoryArchivalNamespaceDefaults{
				State: "disabled",
			},
			Visibility: config.VisibilityArchivalNamespaceDefaults{
				State: "disabled",
			},
		},
	}

	return cfg, nil
}

func newDefaultDynamicConfig() *dynamicconfig.StaticClient {
	dynCfg := dynamicconfig.StaticClient{}

	dynCfg[dynamicconfig.ForceSearchAttributesCacheRefreshOnRead] = []dynamicconfig.ConstrainedValue{{Value: true}}

	return &dynCfg
}

func mustGetService(portProvider *PortProvider, port int) config.Service {
	svc := config.Service{
		RPC: config.RPC{
			GRPCPort:        portProvider.MustGetFreePort(),
			MembershipPort:  portProvider.MustGetFreePort(),
			BindOnLocalHost: true,
			BindOnIP:        "",
		},
	}

	if port != 0 {
		svc.RPC.GRPCPort = port
	}

	return svc
}

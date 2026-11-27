package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func checkHealth(c *cli.Context) error {
	if c.Args().Len() != 0 {
		return errors.New("healthcheck takes no positional arguments")
	}
	cfg, err := loadServerConfig(c)
	if err != nil {
		return fmt.Errorf("load server configuration: %w", err)
	}
	logger := log.NewNoopLogger()
	tlsProvider, err := encryption.NewTLSConfigProviderFromConfig(cfg.Global.TLS, metrics.NoopMetricsHandler, logger, nil)
	if err != nil {
		return fmt.Errorf("load TLS configuration: %w", err)
	}

	healthServices := map[primitives.ServiceName]string{
		primitives.FrontendService:         frontend.WorkflowServiceName,
		primitives.InternalFrontendService: frontend.WorkflowServiceName,
		primitives.HistoryService:          "temporal.api.workflowservice.v1.HistoryService",
		primitives.MatchingService:         "temporal.api.workflowservice.v1.MatchingService",
		primitives.WorkerService:           worker.ServiceName,
	}
	ctx, cancel := context.WithTimeout(c.Context, 4*time.Second)
	defer cancel()
	for _, service := range configuredServices(c) {
		healthService, supported := healthServices[primitives.ServiceName(service)]
		serviceConfig, configured := cfg.Services[service]
		if !supported || !configured {
			return fmt.Errorf("no health endpoint configured for %q", service)
		}
		ip := rpc.GetListenIP(&serviceConfig.RPC, logger)
		if ip == nil || serviceConfig.RPC.GRPCPort == 0 {
			return fmt.Errorf("no fixed gRPC address configured for %q", service)
		}
		// Connect to loopback when the server listens on all interfaces.
		if ip.IsUnspecified() {
			if ip.To4() == nil {
				ip = net.IPv6loopback
			} else {
				ip = net.IPv4(127, 0, 0, 1)
			}
		}
		address := net.JoinHostPort(ip.String(), strconv.Itoa(serviceConfig.RPC.GRPCPort))
		creds, err := healthCredentials(cfg.Global.TLS, tlsProvider, service == string(primitives.FrontendService))
		if err != nil {
			return fmt.Errorf("configure TLS for %s: %w", service, err)
		}
		if err := checkServing(ctx, address, healthService, creds); err != nil {
			return fmt.Errorf("%s at %s: %w", service, address, err)
		}
	}
	return nil
}

func healthCredentials(settings config.RootTLS, provider encryption.TLSConfigProvider, publicFrontend bool) (credentials.TransportCredentials, error) {
	group := settings.Internode
	getClientConfig := provider.GetInternodeClientConfig
	if publicFrontend {
		group = settings.Frontend
		getClientConfig = provider.GetFrontendClientConfig
	}
	if !group.IsServerEnabled() {
		return insecure.NewCredentials(), nil
	}
	tlsConfig, err := getClientConfig()
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: group.Client.ServerName}
	}
	return credentials.NewTLS(tlsConfig), nil
}

func checkServing(ctx context.Context, address, service string, creds credentials.TransportCredentials) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds), grpc.WithNoProxy())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return err
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("health status is %s", response.GetStatus())
	}
	return nil
}

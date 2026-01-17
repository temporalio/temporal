package worker

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	commonconfig "go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/scanner"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestNewServiceHealthCheck(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Create real gRPC components
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	healthServer := health.NewServer()

	logger := log.NewTestLogger()
	hostInfo := membership.NewHostInfoFromAddress("test-host")

	// Create mocks
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().Start().AnyTimes()
	mockClusterMetadata.EXPECT().Stop().AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

	mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
	mockNamespaceRegistry.EXPECT().Start().AnyTimes()
	mockNamespaceRegistry.EXPECT().Stop().AnyTimes()
	mockNamespaceRegistry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).AnyTimes()
	mockNamespaceRegistry.EXPECT().UnregisterStateChangeCallback(gomock.Any()).AnyTimes()

	mockServiceResolver := membership.NewMockServiceResolver(ctrl)
	mockServiceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	mockServiceResolver.EXPECT().RemoveListener(gomock.Any()).AnyTimes()

	mockMembershipMonitor := membership.NewMockMonitor(ctrl)
	mockMembershipMonitor.EXPECT().Start().AnyTimes()
	mockMembershipMonitor.EXPECT().GetResolver(primitives.WorkerService).Return(mockServiceResolver, nil).AnyTimes()

	mockHostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	mockHostInfoProvider.EXPECT().HostInfo().Return(hostInfo).AnyTimes()

	mockMetadataManager := persistence.NewMockMetadataManager(ctrl)
	mockMetadataManager.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{}, nil).AnyTimes()

	mockVisibilityManager := manager.NewMockVisibilityManager(ctrl)
	mockVisibilityManager.EXPECT().Close().AnyTimes()

	mockClientBean := client.NewMockBean(ctrl)
	mockClientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(nil, nil).AnyTimes()

	mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
	mockSdkClientFactory.EXPECT().GetSystemClient().Return(nil).AnyTimes()

	mockWorker := mocksdk.NewMockWorker(ctrl)
	mockWorker.EXPECT().Start().Return(nil).AnyTimes()
	mockWorker.EXPECT().Stop().AnyTimes()
	mockWorker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	mockWorker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockWorker).AnyTimes()

	// Create minimal config
	config := &Config{
		EnableParentClosePolicyWorker: dynamicconfig.GetBoolPropertyFn(false),
		PerNamespaceWorkerStartRate:   dynamicconfig.GetFloatPropertyFn(10),
		PerNamespaceWorkerCount: func(_ string, _ func(int)) (int, func()) {
			return 1, func() {}
		},
		PerNamespaceWorkerOptions: func(_ string, _ func(sdkworker.Options)) (sdkworker.Options, func()) {
			return sdkworker.Options{}, func() {}
		},
		ScannerCfg: &scanner.Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			PersistenceMaxQPS:                      dynamicconfig.GetIntPropertyFn(1),
			Persistence:                            &commonconfig.Persistence{DataStores: map[string]commonconfig.DataStore{"default": {SQL: &commonconfig.SQL{}}}},
			TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(false),
			ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(false),
		},
	}

	// Create worker manager with empty components
	workerMgr := NewWorkerManager(nil, logger, mockSdkClientFactory, hostInfo)

	// Create per-namespace worker manager
	perNsWorkerMgr := NewPerNamespaceWorkerManager(perNamespaceWorkerManagerInitParams{
		Logger:            logger,
		SdkClientFactory:  mockSdkClientFactory,
		NamespaceRegistry: mockNamespaceRegistry,
		HostName:          "test-host",
		Config:            config,
		ClusterMetadata:   mockClusterMetadata,
		Components:        nil,
	})

	// Create the service using NewService
	svc, err := NewService(
		logger,
		config,
		mockSdkClientFactory,
		mockClusterMetadata,
		mockClientBean,
		nil, // clusterMetadataManager
		mockNamespaceRegistry,
		nil, // executionManager
		mockMembershipMonitor,
		mockHostInfoProvider,
		nil, // namespaceReplicationQueue
		metrics.NoopMetricsHandler,
		mockMetadataManager,
		nil, // taskManager
		nil, // historyClient
		workerMgr,
		perNsWorkerMgr,
		mockVisibilityManager,
		nil, // matchingClient
		nsreplication.NewMockTaskExecutor(ctrl),
		serialization.NewSerializer(),
		server,
		listener,
		healthServer,
	)
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Start the service
	svc.Start()

	// Verify health check is working
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	healthClient := healthpb.NewHealthClient(conn)
	resp, err := healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)

	// Stop the service
	svc.Stop()

	// After stop, health check should return NOT_SERVING or connection should fail
	resp, err = healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{
		Service: serviceName,
	})
	if err == nil {
		require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.Status)
	}
	// If err != nil, that's also acceptable as the server has stopped
}

package testcore

import (
	"errors"
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
)

type (
	// TODO (alex): merge this with FunctionalTestBase.
	FunctionalTestSdkSuite struct {
		FunctionalTestBase

		// Suites can override client or worker options by modifying these before calling
		// FunctionalTestSdkSuite.SetupSuite. ClientOptions.HostPort and Namespace cannot be
		// overridden.
		ClientOptions sdkclient.Options
		WorkerOptions worker.Options

		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string
	}
)

// TODO (alex): move this to test_data_converter.go where it is actually used.
var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func (s *FunctionalTestSdkSuite) Worker() worker.Worker {
	return s.worker
}

func (s *FunctionalTestSdkSuite) SdkClient() sdkclient.Client {
	return s.sdkClient
}

func (s *FunctionalTestSdkSuite) TaskQueue() string {
	return s.taskQueue
}

func (s *FunctionalTestSdkSuite) SetupSuite() {
	// these limits are higher in production, but our tests would take too long if we set them that high
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.NumPendingChildExecutionsLimitError.Key():             ClientSuiteLimit,
		dynamicconfig.NumPendingActivitiesLimitError.Key():                  ClientSuiteLimit,
		dynamicconfig.NumPendingCancelRequestsLimitError.Key():              ClientSuiteLimit,
		dynamicconfig.NumPendingSignalsLimitError.Key():                     ClientSuiteLimit,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():      true,
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): ClientSuiteLimit,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                    1 * time.Millisecond,
		nexusoperations.RecordCancelRequestCompletionEvents.Key():           true,
		callbacks.AllowedAddresses.Key():                                    []any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	}

	s.FunctionalTestBase.SetupSuiteWithCluster("testdata/es_cluster.yaml", WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *FunctionalTestSdkSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	s.ClientOptions.HostPort = s.FrontendGRPCAddress()
	s.ClientOptions.Namespace = s.Namespace().String()
	if s.ClientOptions.Logger == nil {
		s.ClientOptions.Logger = log.NewSdkLogger(s.Logger)
	}

	var err error
	s.sdkClient, err = sdkclient.Dial(s.ClientOptions)
	s.NoError(err)
	s.taskQueue = RandomizeStr("tq")

	s.worker = worker.New(s.sdkClient, s.taskQueue, s.WorkerOptions)
	err = s.worker.Start()
	s.NoError(err)
}

func (s *FunctionalTestSdkSuite) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
	s.FunctionalTestBase.TearDownTest()
}

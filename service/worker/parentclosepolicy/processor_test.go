package parentclosepolicy

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.uber.org/mock/gomock"
)

func newTestProcessor(t *testing.T) (*Processor, *mocksdk.MockWorker) {
	t.Helper()

	ctrl := gomock.NewController(t)
	worker := mocksdk.NewMockWorker(ctrl)
	worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()

	clientFactory := sdk.NewMockClientFactory(ctrl)
	clientFactory.EXPECT().GetSystemClient().Return(mocksdk.NewMockClient(ctrl)).AnyTimes()
	clientFactory.EXPECT().
		NewWorker(gomock.Any(), processorTaskQueueName, gomock.Any()).
		Return(worker).
		AnyTimes()

	processor := New(&BootstrapParams{
		Config: Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
		},
		SdkClientFactory: clientFactory,
		MetricsHandler:   metrics.NoopMetricsHandler,
		Logger:           log.NewNoopLogger(),
		CurrentCluster:   "active",
		HostInfo:         membership.NewHostInfoFromAddress("localhost"),
	})
	return processor, worker
}

// The processor keeps no other reference to its worker, so without Stop the
// worker's pollers run until the process exits.
func TestProcessorStop_StopsWorker(t *testing.T) {
	processor, worker := newTestProcessor(t)
	worker.EXPECT().Start().Return(nil)
	worker.EXPECT().Stop().Times(1)

	require.NoError(t, processor.Start())
	processor.Stop()
}

func TestProcessorStop_WithoutStart(t *testing.T) {
	processor, _ := newTestProcessor(t)

	processor.Stop() // must not panic when Start never ran
}

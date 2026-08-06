package parentclosepolicy

import (
	"errors"
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

func TestProcessorStop_StopsWorker(t *testing.T) {
	t.Parallel()

	processor, worker := newTestProcessor(t)
	worker.EXPECT().Start().Return(nil)
	worker.EXPECT().Stop()

	require.NoError(t, processor.Start())
	processor.Stop()
}

func TestProcessorStop_WithoutStart(t *testing.T) {
	t.Parallel()

	processor, _ := newTestProcessor(t)

	require.NotPanics(t, processor.Stop)
}

// Stopping a worker that never started would close its stop channel and shut it
// down, so the processor only takes ownership once Start succeeds.
func TestProcessorStart_FailureLeavesNothingToStop(t *testing.T) {
	t.Parallel()

	processor, worker := newTestProcessor(t)
	// No Stop expectation: gomock fails if the processor stops the worker.
	worker.EXPECT().Start().Return(errors.New("boom"))

	require.Error(t, processor.Start())
	require.NotPanics(t, processor.Stop)
}

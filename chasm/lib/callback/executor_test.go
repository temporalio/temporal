package callback

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func TestBackoffTaskExecutor_GenerateInvocationTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := &BackoffTaskExecutor{
		BackoffTaskExecutorOptions: BackoffTaskExecutorOptions{
			Config: &Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
			MetricsHandler: metrics.NewMockHandler(ctrl),
			Logger:         log.NewNoopLogger(),
		},
	}

	// Create a callback with Nexus variant
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://localhost:8080/callback",
					},
				},
			},
		},
	}

	// Test task generation works correctly
	task, err := executor.generateInvocationTask(callback)
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, "http://localhost:8080/callback", task.Url)
}

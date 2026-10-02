package health

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/stats"
)

func TestEvaluateAndRollupState(t *testing.T) {
	const rpcMethod = "/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution"

	source := Source{Service: primitives.HistoryService, Component: ComponentGRPC}

	type record struct {
		latency time.Duration
		err     error
	}

	testCases := []struct {
		desc     string
		settings Settings
		records  []record

		expectedChecks          []*healthspb.HealthCheck
		expectedState           enumspb.HealthState
		expectedUnenforcedState enumspb.HealthState
	}{
		{
			desc: "overall thresholds satisfied",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds:  []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
					Enforced:            true,
				},
			},
			records: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     100,
					Threshold: 2000,
					Enforced:  true,
				},
				{
					CheckType: "history.grpc.overall.error_ratio",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     0,
					Threshold: 0.1,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_SERVING,
		},
		{
			desc: "group latency over threshold while overall stays healthy",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					Enforced:           true,
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{rpcMethod},
						Thresholds: Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
							QuantileThresholds:  []QuantileThreshold{{Quantile: 0.99, Threshold: 200 * time.Millisecond}},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
							Enforced:            true,
						},
					},
				},
			},
			// 900ms is under the overall 2s threshold but well over the group's 200ms
			records: []record{
				{900 * time.Millisecond, nil},
				{900 * time.Millisecond, nil},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     900,
					Threshold: 2000,
					Enforced:  true,
				},
				// the group is enforced, so this is what drives the overall NOT_SERVING
				{
					CheckType: "history.grpc.group.critical.latency.p99",
					State:     enumspb.HEALTH_STATE_NOT_SERVING,
					Value:     900,
					Threshold: 200,
					Enforced:  true,
				},
				{
					CheckType: "history.grpc.group.critical.error_ratio",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     0,
					Threshold: 0.1,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_NOT_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			desc: "error ratio over threshold",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds:  []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
					Enforced:            true,
				},
			},
			// 1 of 2 calls failed -> 0.5 error ratio, over the 0.1 threshold
			records: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, errors.New("boom")},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     100,
					Threshold: 2000,
					Enforced:  true,
				},
				{
					CheckType: "history.grpc.overall.error_ratio",
					State:     enumspb.HEALTH_STATE_NOT_SERVING,
					Value:     0.5,
					Threshold: 0.1,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_NOT_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			desc: "unenforced breach only moves the unenforced state",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []QuantileThreshold{{Quantile: 0.99, Threshold: 500 * time.Millisecond}},
					Enforced:           false,
				},
			},
			// 600ms is over the threshold, but the overall bucket is not enforced
			records: []record{
				{600 * time.Millisecond, nil},
				{600 * time.Millisecond, nil},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_NOT_SERVING,
					Value:     600,
					Threshold: 500,
					Enforced:  false,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			desc: "no records reports healthy",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds:  []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
					Enforced:            true,
				},
			},
			records: nil,
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     0,
					Threshold: 2000,
					Enforced:  true,
				},
				{
					CheckType: "history.grpc.overall.error_ratio",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     0,
					Threshold: 0.1,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_SERVING,
		},
		{
			desc:                    "empty settings produces no checks",
			settings:                Settings{},
			records:                 []record{{100 * time.Millisecond, nil}},
			expectedChecks:          nil,
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_SERVING,
		},
		{
			desc: "group without a window config skips its latency check",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					Enforced:           true,
				},
				Groups: []Group{
					{
						Name: "critical",
						Keys: []string{rpcMethod},
						// no WindowConfig, so the aggregator never builds a latency bucket and
						// the quantile threshold below has nothing to read
						Thresholds: Thresholds{
							QuantileThresholds:  []QuantileThreshold{{Quantile: 0.99, Threshold: 200 * time.Millisecond}},
							ErrorRatioThreshold: &ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
							Enforced:            true,
						},
					},
				},
			},
			records: []record{
				{900 * time.Millisecond, nil},
				{900 * time.Millisecond, nil},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     900,
					Threshold: 2000,
					Enforced:  true,
				},
				{
					CheckType: "history.grpc.group.critical.error_ratio",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     0,
					Threshold: 0.1,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_SERVING,
		},
		{
			desc: "nil error ratio threshold skips the error ratio check",
			settings: Settings{
				Overall: Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					Enforced:           true,
				},
			},
			// the error would count toward the ratio, but no threshold is configured
			records: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, errors.New("boom")},
			},
			expectedChecks: []*healthspb.HealthCheck{
				{
					CheckType: "history.grpc.overall.latency.p99",
					State:     enumspb.HEALTH_STATE_SERVING,
					Value:     100,
					Threshold: 2000,
					Enforced:  true,
				},
			},
			expectedState:           enumspb.HEALTH_STATE_SERVING,
			expectedUnenforcedState: enumspb.HEALTH_STATE_SERVING,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			agg := NewSignalAggregator(
				log.NewNoopLogger(),
				func() Settings { return tc.settings },
			)

			for _, r := range tc.records {
				agg.Record(rpcMethod, r.latency, r.err)
			}

			checks := Evaluate(agg, tc.settings, source)
			require.Equal(t, tc.expectedChecks, checks)

			state, unenforcedState := RollupState(checks)
			require.Equal(t, tc.expectedState, state)
			require.Equal(t, tc.expectedUnenforcedState, unenforcedState)
		})
	}
}

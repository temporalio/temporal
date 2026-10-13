package interceptor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	rpcerrdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"
)

func retryInfoDetail(err error) *rpcerrdetails.RetryInfo {
	for _, detail := range serviceerror.ToStatus(err).Details() {
		if retryInfo, ok := detail.(*rpcerrdetails.RetryInfo); ok {
			return retryInfo
		}
	}
	return nil
}

func addRetryInfo(t testing.TB, base *serviceerror.ResourceExhausted, delay time.Duration) *serviceerror.ResourceExhausted {
	t.Helper()
	st, err := base.Status().WithDetails(&rpcerrdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	require.NoError(t, err)
	floored, ok := serviceerror.FromStatus(st).(*serviceerror.ResourceExhausted)
	require.True(t, ok)
	return floored
}

func TestWithResourceExhaustedRetryDelayFloor(t *testing.T) {
	t.Parallel()

	base := ErrNamespaceRateLimitServerBusy

	for _, tc := range []struct {
		name      string
		floor     time.Duration
		wantDelay time.Duration
		wantSame  bool
	}{
		{name: "nil floor", wantSame: true},
		{name: "zero floor", floor: 0, wantSame: true},
		{name: "negative floor", floor: -time.Second, wantSame: true},
		{name: "floor", floor: 2 * time.Second, wantDelay: 2 * time.Second},
		{name: "large floor", floor: time.Hour, wantDelay: time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)

			var floorFn dynamicconfig.DurationPropertyFnWithNamespaceFilter
			if tc.name != "nil floor" {
				floorFn = func(string) time.Duration { return tc.floor }
			}

			got := withResourceExhaustedRetryDelayFloor(base, namespace.Name("floor-ns"), floorFn, mh, "TestOperation")
			if tc.wantSame {
				require.Same(t, base, got)
				require.Nil(t, retryInfoDetail(got))
				require.Empty(t, capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
				return
			}

			require.NotSame(t, base, got)
			require.Equal(t, base.Cause, got.Cause)
			require.Equal(t, base.Scope, got.Scope)
			require.Equal(t, base.Message, got.Message)
			details := serviceerror.ToStatus(got).Proto().GetDetails()
			require.Len(t, details, 2)
			require.True(t, details[len(details)-1].MessageIs(&rpcerrdetails.RetryInfo{}))
			require.Len(t, serviceerror.ToStatus(base).Proto().GetDetails(), 1)
			retryInfo := retryInfoDetail(got)
			require.NotNil(t, retryInfo)
			require.Equal(t, tc.wantDelay, retryInfo.GetRetryDelay().AsDuration())
			require.Nil(t, retryInfoDetail(base))
			recordings := capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name())
			require.Len(t, recordings, 1)
			require.Equal(t, int64(1), recordings[0].Value)
			require.Equal(t, map[string]string{
				"namespace": "floor-ns",
				"operation": "TestOperation",
			}, recordings[0].Tags)
		})
	}
}

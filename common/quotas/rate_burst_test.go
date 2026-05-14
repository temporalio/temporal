package quotas_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas"
)

type fakeRateBurst struct {
	rate  float64
	burst int
}

func (f fakeRateBurst) Rate() float64 { return f.rate }
func (f fakeRateBurst) Burst() int    { return f.burst }

// Burst must equal base burst, not (ratio * base). With ratio=0.2 the prior
// `int(ratio * base)` collapsed to 0 for base<5 and denied all operator calls.
func TestOperatorRateBurst_BurstPassesThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ratio     float64
		baseBurst int
	}{
		{"small_base_burst", 0.2, 1},
		{"medium_base_burst", 0.2, 4},
		{"larger_base_burst", 0.2, 10},
		{"full_ratio", 1.0, 7},
		{"zero_ratio", 0.0, 100},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ratio := tc.ratio
			b := quotas.NewOperatorRateBurst(
				fakeRateBurst{rate: 100, burst: tc.baseBurst},
				func() float64 { return ratio },
			)
			require.Equal(t, tc.baseBurst, b.Burst())
		})
	}
}

func TestOperatorRateBurst_RateAppliesRatio(t *testing.T) {
	t.Parallel()

	b := quotas.NewOperatorRateBurst(
		fakeRateBurst{rate: 50, burst: 10},
		func() float64 { return 0.2 },
	)
	require.InDelta(t, 10.0, b.Rate(), 1e-9)
}

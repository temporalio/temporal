package taskqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

func TestNeedsRelease(t *testing.T) {
	testCases := []struct {
		name string
		ref  *taskqueuespb.LimiterRef
		want bool
	}{
		{
			name: "nil",
		},
		{
			name: "unspecified",
			ref:  &taskqueuespb.LimiterRef{},
		},
		{
			name: "concurrency",
			ref: &taskqueuespb.LimiterRef{
				LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY,
			},
			want: true,
		},
		{
			name: "unknown",
			ref: &taskqueuespb.LimiterRef{
				LimiterType: enumsspb.LimiterType(100),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, NeedsRelease(testCase.ref))
		})
	}
}

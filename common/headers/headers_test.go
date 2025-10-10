package headers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPropagate_CreateNewOutgoingContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: ">21.04.16",
		ClientNameHeaderName:              "28.08.14",
		SupportedFeaturesHeaderName:       "my-feature",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	require.Equal(t, "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(t, ">21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(t, "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func TestPropagate_CreateNewOutgoingContext_SomeMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName: "22.08.78",
		ClientNameHeaderName:    "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	require.Equal(t, "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(t, 0, len(md.Get(SupportedServerVersionsHeaderName)))
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(t, 0, len(md.Get(SupportedFeaturesHeaderName)))
}

func TestPropagate_UpdateExistingEmptyOutgoingContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
		SupportedFeaturesHeaderName:       "my-feature",
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	require.Equal(t, "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(t, "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(t, "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func TestPropagate_UpdateExistingNonEmptyOutgoingContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "07.08.78",   // Must be ignored
		SupportedServerVersionsHeaderName: "<07.04.16",  // Must be ignored
		SupportedFeaturesHeaderName:       "my-feature", // Passed through
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	require.Equal(t, "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(t, "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(t, "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func TestPropagate_EmptyIncomingContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	require.Equal(t, "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(t, "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
}

func TestIsExperimentEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headerValues    []string // multiple header values
		checkExperiment string
		expected        bool
	}{
		{
			name:            "no header",
			headerValues:    nil,
			checkExperiment: "test-experiment",
			expected:        false,
		},
		{
			name:            "empty header",
			headerValues:    []string{""},
			checkExperiment: "test-experiment",
			expected:        false,
		},
		{
			name:            "single experiment - match",
			headerValues:    []string{"chasm-sch"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "single experiment - no match",
			headerValues:    []string{"chasm-sch"},
			checkExperiment: "other-experiment",
			expected:        false,
		},
		{
			name:            "case insensitive - lowercase check",
			headerValues:    []string{"Chasm-Sch"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "case insensitive - uppercase check",
			headerValues:    []string{"Chasm-Sch"},
			checkExperiment: "CHASM-SCH",
			expected:        true,
		},
		{
			name:            "case insensitive - mixed case check",
			headerValues:    []string{"Chasm-Sch"},
			checkExperiment: "ChAsM-ScH",
			expected:        true,
		},
		{
			name:            "multiple experiments comma separated - first",
			headerValues:    []string{"chasm-sch,other-exp,third-exp"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "multiple experiments comma separated - middle",
			headerValues:    []string{"chasm-sch,other-exp,third-exp"},
			checkExperiment: "other-exp",
			expected:        true,
		},
		{
			name:            "multiple experiments comma separated - last",
			headerValues:    []string{"chasm-sch,other-exp,third-exp"},
			checkExperiment: "third-exp",
			expected:        true,
		},
		{
			name:            "multiple experiments comma separated - not present",
			headerValues:    []string{"chasm-sch,other-exp,third-exp"},
			checkExperiment: "not-present",
			expected:        false,
		},
		{
			name:            "multiple experiments with spaces",
			headerValues:    []string{"chasm-sch, other-exp , third-exp"},
			checkExperiment: "other-exp",
			expected:        true,
		},
		{
			name:            "wildcard - any experiment",
			headerValues:    []string{"*"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "wildcard - different experiment",
			headerValues:    []string{"*"},
			checkExperiment: "any-experiment",
			expected:        true,
		},
		{
			name:            "wildcard with other experiments",
			headerValues:    []string{"chasm-sch,*,other-exp"},
			checkExperiment: "any-experiment",
			expected:        true,
		},
		{
			name:            "multiple header values - first header",
			headerValues:    []string{"chasm-sch", "other-exp"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "multiple header values - second header",
			headerValues:    []string{"chasm-sch", "other-exp"},
			checkExperiment: "other-exp",
			expected:        true,
		},
		{
			name:            "multiple header values - not present",
			headerValues:    []string{"chasm-sch", "other-exp"},
			checkExperiment: "not-present",
			expected:        false,
		},
		{
			name:            "multiple headers with comma separated - first experiment",
			headerValues:    []string{"chasm-sch,other-exp", "third-exp", "fourth-exp,fifth-exp"},
			checkExperiment: "chasm-sch",
			expected:        true,
		},
		{
			name:            "multiple headers with comma separated - middle experiment",
			headerValues:    []string{"chasm-sch,other-exp", "third-exp", "fourth-exp,fifth-exp"},
			checkExperiment: "third-exp",
			expected:        true,
		},
		{
			name:            "multiple headers with comma separated - last experiment",
			headerValues:    []string{"chasm-sch,other-exp", "third-exp", "fourth-exp,fifth-exp"},
			checkExperiment: "fifth-exp",
			expected:        true,
		},
		{
			name:            "multiple headers with comma separated - not present",
			headerValues:    []string{"chasm-sch,other-exp", "third-exp", "fourth-exp,fifth-exp"},
			checkExperiment: "not-present",
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			md := metadata.MD{}
			for _, val := range tt.headerValues {
				md.Append(ExperimentalHeaderName, val)
			}
			ctx = metadata.NewIncomingContext(ctx, md)

			result := IsExperimentEnabled(ctx, tt.checkExperiment)
			require.Equal(t, tt.expected, result)
		})
	}
}

package headers

import (
	"context"
	"strings"
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
	require.Empty(t, md.Get(SupportedServerVersionsHeaderName))
	require.Equal(t, "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Empty(t, md.Get(SupportedFeaturesHeaderName))
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

func TestIsExperimentRequested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headerValues    []string
		checkExperiment string
		expected        bool
	}{
		{
			name:            "no header returns false",
			headerValues:    nil,
			checkExperiment: "chasm-scheduler",
			expected:        false,
		},
		{
			name:            "exact match returns true",
			headerValues:    []string{"chasm-scheduler"},
			checkExperiment: "chasm-scheduler",
			expected:        true,
		},
		{
			name:            "comma separated list finds match",
			headerValues:    []string{"chasm-scheduler, other-exp, third-exp"},
			checkExperiment: "other-exp",
			expected:        true,
		},
		{
			name:            "wildcard matches any experiment",
			headerValues:    []string{"*"},
			checkExperiment: "any-experiment",
			expected:        true,
		},
		{
			name:            "wildcard in list matches",
			headerValues:    []string{"chasm-scheduler,*,other-exp"},
			checkExperiment: "random-experiment",
			expected:        true,
		},
		{
			name:            "multiple header values finds match",
			headerValues:    []string{"chasm-scheduler", "other-exp,third-exp"},
			checkExperiment: "third-exp",
			expected:        true,
		},
		{
			name:            "max experiment size limit match",
			headerValues:    []string{strings.Repeat("a,", 49)},
			checkExperiment: "a",
			expected:        true, // 98 chars, under 100 char limit
		},
		{
			name:            "at max experiment size limit no match",
			headerValues:    []string{strings.Repeat("a,", 51)},
			checkExperiment: "a",
			expected:        false, // exceeds 100 char limit, should be skipped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			md := metadata.MD{}
			for _, val := range tt.headerValues {
				md.Append(ExperimentHeaderName, val)
			}
			ctx = metadata.NewIncomingContext(ctx, md)

			result := IsExperimentRequested(ctx, tt.checkExperiment)
			require.Equal(t, tt.expected, result)
		})
	}
}

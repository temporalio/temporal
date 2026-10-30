package mixedbrain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveReleaseVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		serverVersion string
		tags          []string
		want          string
	}{
		{
			name:          "prefers stable over pre-release",
			serverVersion: "1.31.0",
			tags:          []string{"v1.30.0", "v1.30.1-184.3", "v1.30.1"},
			want:          "1.30.1",
		},
		{
			name:          "falls back to pre-release when no stable",
			serverVersion: "1.31.0",
			tags:          []string{"v1.30.1-184.3", "v1.30.0-100.1", "v1.29.5"},
			want:          "1.30.1-184.3",
		},
		{
			name:          "filters to previous minor only",
			serverVersion: "1.31.0",
			tags:          []string{"v1.30.2", "v1.29.5", "v1.31.0", "v2.30.0"},
			want:          "1.30.2",
		},
		{
			name:          "picks highest patch",
			serverVersion: "1.31.0",
			tags:          []string{"v1.30.0", "v1.30.3", "v1.30.1"},
			want:          "1.30.3",
		},
		{
			name:          "skips invalid tags",
			serverVersion: "1.31.0",
			tags:          []string{"v1.30.0", "not-a-version", "v1.30.1"},
			want:          "1.30.1",
		},
		{
			name:          "zero when no matching tags",
			serverVersion: "1.31.0",
			tags:          []string{"v1.29.0", "v1.28.0"},
			want:          "0.0.0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveReleaseVersion(tc.serverVersion, tc.tags)
			require.Equal(t, tc.want, got.String())
		})
	}
}

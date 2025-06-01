package testutils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRepoRootDirectory(t *testing.T) {
	t.Parallel()
	t.Run("env var set", func(t *testing.T) {
		directory := GetRepoRootDirectory(WithGetenv(func(s string) string {
			assert.Equal(t, "TEMPORAL_ROOT", s)
			return "/tmp/temporal"
		}))
		assert.Equal(t, "/tmp/temporal", directory)
	})

	t.Run("env var not set", func(t *testing.T) {
		directory := GetRepoRootDirectory(WithGetenv(func(s string) string {
			return ""
		}))
		assert.True(
			t,
			strings.HasSuffix(directory, "temporal") || strings.HasSuffix(directory, "temporal/"),
			directory,
		)
	})
}

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDirs(t *testing.T) {
	dirs := PathsByDir("cassandra")
	requireContains(t, []string{
		"cassandra/temporal",
	}, dirs)

	dirs = PathsByDir("mysql")
	requireContains(t, []string{
		"mysql/v8/temporal",
		"mysql/v8/visibility",
	}, dirs)

	dirs = PathsByDir("postgresql")
	requireContains(t, []string{
		"postgresql/v12/temporal",
		"postgresql/v12/visibility",
	}, dirs)
}

func requireContains(t *testing.T, expected []string, actual []string) {
	for _, v := range expected {
		require.Contains(t, actual, v)
	}
}

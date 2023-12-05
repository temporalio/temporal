package schema

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSchemaDirs(t *testing.T) {
	dirs := Paths("cassandra")
	require.Equal(t, []string{"cassandra/temporal", "cassandra/visibility"}, dirs)

	dirs = Paths("mysql")
	require.Equal(t, []string{
		"mysql/v57/temporal",
		"mysql/v57/visibility",
		"mysql/v8/temporal",
		"mysql/v8/visibility",
	}, dirs)

	dirs = Paths("postgresql")
	require.Equal(t, []string{
		"postgresql/v12/temporal",
		"postgresql/v12/visibility",
		"postgresql/v96/temporal",
		"postgresql/v96/visibility",
	}, dirs)
}

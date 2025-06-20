package sqlplugin

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDbFields_LastField_Version(t *testing.T) {
	lastField := DbFields[len(DbFields)-1]
	require.Equal(t, VersionColumnName, lastField)
}

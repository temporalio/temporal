package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Tier-1 (server-free): the standalone-activity lifecycle is sound — valid and, via the
// generic Lifecycle, total over its (state × event) grid.
func TestActivityLifecycle_ValidAndTotal(t *testing.T) {
	require.NoError(t, NewActivityLifecycle().Validate())
}

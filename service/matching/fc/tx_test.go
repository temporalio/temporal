package fc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNilTx(t *testing.T) {
	var tx *Tx
	require.NoError(t, tx.SkipCheck())
}

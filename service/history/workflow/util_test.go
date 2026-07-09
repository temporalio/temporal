package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func TestToChasmTransactionPolicy(t *testing.T) {
	require.Equal(t, chasm.TransactionPolicyActive, toChasmTransactionPolicy(historyi.TransactionPolicyActive))
	require.Equal(t, chasm.TransactionPolicyPassive, toChasmTransactionPolicy(historyi.TransactionPolicyPassive))
}

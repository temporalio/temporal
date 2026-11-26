package serverbuggify

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestBuggifiableSettings(t *testing.T) {
	t.Parallel()

	for _, b := range buggifiable {
		t.Run(b.setting.Key().String(), func(t *testing.T) {
			t.Parallel()
			require.NotEmpty(t, b.values)
			for _, v := range b.values {
				require.NoError(t, b.setting.Validate(v), v)
			}
		})
	}

	// Every setting is listed once.
	marked := BuggifiableSettings()
	require.Len(t, marked, len(buggifiable))
	require.Equal(t, []any{1, 2, 10}, marked[dynamicconfig.TransferTaskBatchSize.Key()])
	require.NotContains(t, marked, dynamicconfig.HistoryMaxPageSize.Key())
}

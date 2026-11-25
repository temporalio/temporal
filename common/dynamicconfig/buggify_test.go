package dynamicconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

var buggifiableDuration = dynamicconfig.Buggifiable(
	dynamicconfig.NewNamespaceDurationSetting("test.buggifiableDuration", time.Minute, ""),
	time.Second, 5*time.Second,
)

func TestBuggifiable(t *testing.T) {
	t.Parallel()

	marked := dynamicconfig.BuggifiableSettings()
	require.Equal(t, []any{time.Second, 5 * time.Second}, marked[buggifiableDuration.Key()])
	require.Equal(t, []any{1, 2, 10}, marked[dynamicconfig.TransferTaskBatchSize.Key()])
	require.NotContains(t, marked, dynamicconfig.HistoryMaxPageSize.Key())

	// The marker doesn't change the setting.
	require.Equal(t, time.Minute, buggifiableDuration.Get(dynamicconfig.NewNoopCollection())("ns"))

	require.Panics(t, func() { dynamicconfig.Buggifiable(buggifiableDuration) })
	require.Panics(t, func() { dynamicconfig.Buggifiable(buggifiableDuration, time.Hour) })
}

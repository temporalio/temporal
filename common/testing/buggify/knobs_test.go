package buggify

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	fpbuggify "go.temporal.io/testx/failpoint/buggify"
)

func TestChooseKnobs(t *testing.T) {
	t.Parallel()

	marked := map[dynamicconfig.Key][]any{}
	for i := range 400 {
		marked[dynamicconfig.MakeKey(fmt.Sprintf("test.knob%d", i))] = []any{1, 2, 3}
	}
	config := Defaults()
	config.Seed = 11

	chosen := ChooseKnobs(config, marked)
	require.Equal(t, chosen, ChooseKnobs(config, marked))
	require.InDelta(t, 100, len(chosen), 30)
	counts := map[any]int{}
	for key, value := range chosen {
		require.Contains(t, marked[key], value)
		require.True(t, config.Activated(KnobPrefix+key.String()))
		counts[value]++
	}
	require.Len(t, counts, 3)

	other := config
	other.Seed = 12
	require.NotEqual(t, chosen, ChooseKnobs(other, marked))

	config.Exclude = []string{KnobPrefix + "*"}
	require.Empty(t, ChooseKnobs(config, marked))
	require.Empty(t, ChooseKnobs(fpbuggify.Config{}, marked))
}

// staticClient returns the same slices on every call, like the file-based client.
type staticClient map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue

func (c staticClient) GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue { return c[key] }

func TestKnobClient(t *testing.T) {
	t.Parallel()

	knob := dynamicconfig.MakeKey("test.knob")
	other := dynamicconfig.MakeKey("test.other")
	namespaceValue := dynamicconfig.ConstrainedValue{Constraints: dynamicconfig.Constraints{Namespace: "ns"}, Value: 5}
	globalValue := dynamicconfig.ConstrainedValue{Value: 6}
	knobValue := dynamicconfig.ConstrainedValue{Value: 1}

	t.Run("adds the knob unless a value without constraints is set", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name  string
			inner []dynamicconfig.ConstrainedValue
			want  []dynamicconfig.ConstrainedValue
		}{
			{name: "unset", inner: nil, want: []dynamicconfig.ConstrainedValue{knobValue}},
			{name: "constrained", inner: []dynamicconfig.ConstrainedValue{namespaceValue}, want: []dynamicconfig.ConstrainedValue{namespaceValue, knobValue}},
			{name: "global", inner: []dynamicconfig.ConstrainedValue{namespaceValue, globalValue}, want: []dynamicconfig.ConstrainedValue{namespaceValue, globalValue}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				client := NewKnobClient(staticClient{knob: tc.inner, other: tc.inner}, map[dynamicconfig.Key]any{knob: 1})
				got := client.GetValue(knob)
				require.Equal(t, tc.want, got)
				// The collection caches conversions per slice, so the client returns the same one.
				require.Same(t, &got[0], &client.GetValue(knob)[0])
				require.Equal(t, tc.inner, client.GetValue(other))
			})
		}
	})

	t.Run("works with the dynamic config collection", func(t *testing.T) {
		t.Parallel()
		inner := dynamicconfig.NewMemoryClient()
		client := NewKnobClient(inner, map[dynamicconfig.Key]any{dynamicconfig.TransferTaskBatchSize.Key(): 2})
		_, notifying := client.(dynamicconfig.NotifyingClient)
		require.True(t, notifying)
		_, notifying = NewKnobClient(staticClient{}, nil).(dynamicconfig.NotifyingClient)
		require.False(t, notifying)

		collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
		collection.Start()
		t.Cleanup(collection.Stop)
		updates := make(chan int, 10)
		get := dynamicconfig.TransferTaskBatchSize.Get(collection)
		value, cancel := dynamicconfig.TransferTaskBatchSize.Subscribe(collection)(func(v int) { updates <- v })
		t.Cleanup(cancel)
		require.Equal(t, 2, value)
		require.Equal(t, 2, get())

		cleanup := inner.OverrideSetting(dynamicconfig.TransferTaskBatchSize, 50)
		require.Equal(t, 50, <-updates)
		require.Equal(t, 50, get())
		cleanup()
		require.Equal(t, 2, <-updates)
		require.Equal(t, 2, get())
	})
}

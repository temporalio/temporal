package serverbuggify

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/testx/failpoint/buggify"
)

// settingsConfig activates a quarter of the settings, like the standard keys do.
func settingsConfig(seed uint64) buggify.Config {
	return buggify.Config{Seed: seed, Keys: []buggify.Key{{Match: SettingPrefix + "*", ActivationProbability: 0.25, FireProbability: 0.25}}}
}

func TestChooseSettings(t *testing.T) {
	t.Parallel()

	marked := map[dynamicconfig.Key][]any{}
	for i := range 400 {
		marked[dynamicconfig.MakeKey(fmt.Sprintf("test.setting%d", i))] = []any{1, 2, 3}
	}
	config := settingsConfig(11)

	chosen := ChooseSettings(config, marked)
	require.Equal(t, chosen, ChooseSettings(config, marked))
	require.InDelta(t, 100, len(chosen), 30)
	counts := map[any]int{}
	for key, value := range chosen {
		require.Contains(t, marked[key], value)
		require.True(t, config.Activated(SettingPrefix+key.String()))
		counts[value]++
	}
	require.Len(t, counts, 3)

	other := config
	other.Seed = 12
	require.NotEqual(t, chosen, ChooseSettings(other, marked))

	// A key that no entry matches is never randomized, and neither is one that a narrow entry
	// before the broad one keeps from activating.
	config.Keys = nil
	require.Empty(t, ChooseSettings(config, marked))
	config.Keys = []buggify.Key{
		{Match: SettingPrefix + "test.*", ActivationProbability: 0},
		{Match: "*", ActivationProbability: 1},
	}
	require.Empty(t, ChooseSettings(config, marked))
	require.Empty(t, ChooseSettings(buggify.Config{}, marked))
}

// staticClient returns the same slices on every call, like the file-based client.
type staticClient map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue

func (c staticClient) GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue { return c[key] }

func TestRandomizingClient(t *testing.T) {
	t.Parallel()

	setting := dynamicconfig.MakeKey("test.setting")
	other := dynamicconfig.MakeKey("test.other")
	namespaceValue := dynamicconfig.ConstrainedValue{Constraints: dynamicconfig.Constraints{Namespace: "ns"}, Value: 5}
	globalValue := dynamicconfig.ConstrainedValue{Value: 6}
	chosenValue := dynamicconfig.ConstrainedValue{Value: 1}

	t.Run("adds the chosen value unless a value without constraints is set", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name  string
			inner []dynamicconfig.ConstrainedValue
			want  []dynamicconfig.ConstrainedValue
		}{
			{name: "unset", inner: nil, want: []dynamicconfig.ConstrainedValue{chosenValue}},
			{name: "constrained", inner: []dynamicconfig.ConstrainedValue{namespaceValue}, want: []dynamicconfig.ConstrainedValue{namespaceValue, chosenValue}},
			{name: "global", inner: []dynamicconfig.ConstrainedValue{namespaceValue, globalValue}, want: []dynamicconfig.ConstrainedValue{namespaceValue, globalValue}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				client := NewRandomizingClient(staticClient{setting: tc.inner, other: tc.inner}, map[dynamicconfig.Key]any{setting: 1})
				got := client.GetValue(setting)
				require.Equal(t, tc.want, got)
				// The collection caches conversions per slice, so the client returns the same one.
				require.Same(t, &got[0], &client.GetValue(setting)[0])
				require.Equal(t, tc.inner, client.GetValue(other))
			})
		}
	})

	t.Run("works with the dynamic config collection", func(t *testing.T) {
		t.Parallel()
		inner := dynamicconfig.NewMemoryClient()
		client := NewRandomizingClient(inner, map[dynamicconfig.Key]any{dynamicconfig.TransferTaskBatchSize.Key(): 2})
		_, notifying := client.(dynamicconfig.NotifyingClient)
		require.True(t, notifying)
		_, notifying = NewRandomizingClient(staticClient{}, nil).(dynamicconfig.NotifyingClient)
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

// A seed reproduces a run only if it picks the same values in every process, so this pins the
// choices of one seed.
func TestChooseSettings_StableForASeed(t *testing.T) {
	t.Parallel()

	marked := map[dynamicconfig.Key][]any{}
	for _, key := range []string{
		"history.transferTaskBatchSize", "history.timerTaskBatchSize", "matching.maxTaskBatchSize",
		"a.one", "a.two", "a.three", "a.four", "a.five", "a.six",
	} {
		marked[dynamicconfig.MakeKey(key)] = []any{1, 2, 10}
	}
	config := settingsConfig(42)

	require.Equal(t, map[dynamicconfig.Key]any{
		dynamicconfig.MakeKey("matching.maxTaskBatchSize"): 2,
		dynamicconfig.MakeKey("a.one"):                     2,
		dynamicconfig.MakeKey("a.six"):                     10,
	}, ChooseSettings(config, marked))
}

// A value that dynamic config sets for a task queue wins over the value that BUGGIFY picked for
// the setting, and the pick still applies to the other task queues.
func TestRandomizingClient_ConstrainedValueWinsOverTheChosenValue(t *testing.T) {
	t.Parallel()

	key := dynamicconfig.MatchingMaxTaskBatchSize.Key()
	inner := staticClient{key: {{Constraints: dynamicconfig.Constraints{Namespace: "ns"}, Value: 5}}}
	collection := dynamicconfig.NewCollection(NewRandomizingClient(inner, map[dynamicconfig.Key]any{key: 2}), log.NewNoopLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	get := dynamicconfig.MatchingMaxTaskBatchSize.Get(collection)

	require.Equal(t, 5, get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 2, get("other", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

// Settings are read from many goroutines while dynamic config changes; run with -race.
func TestRandomizingClient_Concurrent(t *testing.T) {
	t.Parallel()

	key := dynamicconfig.TransferTaskBatchSize.Key()
	inner := dynamicconfig.NewMemoryClient()
	client := NewRandomizingClient(inner, map[dynamicconfig.Key]any{key: 2})
	collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	collection.Start()
	t.Cleanup(collection.Stop)
	get := dynamicconfig.TransferTaskBatchSize.Get(collection)
	_, cancel := dynamicconfig.TransferTaskBatchSize.Subscribe(collection)(func(int) {})
	t.Cleanup(cancel)

	// The readers report what they saw, so the assertions run in the test goroutine.
	unexpected := make(chan int, 8*500)
	empty := make(chan struct{}, 8*500)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				// The chosen value, or the value that the writer below sets.
				if v := get(); v != 2 && v != 50 && v != 60 {
					unexpected <- v
				}
				if len(client.GetValue(key)) == 0 {
					empty <- struct{}{}
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 200 {
			cleanup := inner.OverrideSetting(dynamicconfig.TransferTaskBatchSize, 50+10*(i%2))
			cleanup()
		}
	})
	wg.Wait()
	require.Empty(t, unexpected)
	require.Empty(t, empty)
}

// A setting that has only constrained values gets the chosen value added, which goes through the
// cache of patched values. Reads of such a setting race with each other and with updates; run with
// -race. The readers check the constrained value and the chosen value.
func TestRandomizingClient_ConcurrentConstrained(t *testing.T) {
	t.Parallel()

	key := dynamicconfig.MatchingMaxTaskBatchSize.Key()
	constrained := func(namespace string, value int) dynamicconfig.ConstrainedValue {
		return dynamicconfig.ConstrainedValue{Constraints: dynamicconfig.Constraints{Namespace: namespace}, Value: value}
	}
	read := func(t *testing.T, client dynamicconfig.Client, update func()) {
		collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
		collection.Start()
		t.Cleanup(collection.Stop)
		get := dynamicconfig.MatchingMaxTaskBatchSize.Get(collection)

		// The readers report what they saw, so the assertions run in the test goroutine.
		unexpected := make(chan string, 8*500)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 500 {
					if v := get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW); v != 5 {
						unexpected <- fmt.Sprintf("ns: %d, want the constrained value 5", v)
					}
					if v := get("other", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW); v != 2 {
						unexpected <- fmt.Sprintf("other: %d, want the chosen value 2", v)
					}
				}
			})
		}
		if update != nil {
			wg.Go(update)
		}
		wg.Wait()
		close(unexpected)
		for msg := range unexpected {
			require.Fail(t, msg)
		}
	}

	t.Run("values that don't change", func(t *testing.T) {
		t.Parallel()
		// The same slice on every read, so the readers share one patched value.
		inner := staticClient{key: {constrained("ns", 5)}}
		read(t, NewRandomizingClient(inner, map[dynamicconfig.Key]any{key: 2}), nil)
	})

	t.Run("values that update", func(t *testing.T) {
		t.Parallel()
		inner := dynamicconfig.NewMemoryClient()
		client := NewRandomizingClient(inner, map[dynamicconfig.Key]any{key: 2})
		notifying, ok := client.(dynamicconfig.NotifyingClient)
		require.True(t, ok)
		cancel := notifying.Subscribe(func(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {})
		t.Cleanup(cancel)

		// The constrained value is present for the whole run of the readers, while another override
		// of the setting comes and goes and publishes the patched values.
		cleanup := inner.PartialOverrideValue(key, []dynamicconfig.ConstrainedValue{constrained("ns", 5)})
		t.Cleanup(cleanup)
		read(t, client, func() {
			for range 200 {
				inner.PartialOverrideValue(key, []dynamicconfig.ConstrainedValue{constrained("ns2", 7)})()
			}
		})
	})
}

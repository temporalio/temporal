package dynamicconfig

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestGradualChangeValue_BeforeAfter(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}

	for _, key := range []string{"key1", "key2", "key3"} {
		assert.Equal(t, "old", gc.Value([]byte(key), start.Add(-time.Hour)))
		assert.Equal(t, "old", gc.Value([]byte(key), start))
		assert.Equal(t, "new", gc.Value([]byte(key), end))
		assert.Equal(t, "new", gc.Value([]byte(key), end.Add(time.Hour)))
	}
}

func TestGradualChangeValue_DuringTransition(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}

	midpoint := start.Add(5 * 24 * time.Hour)

	oldCount, newCount := 0, 0
	for i := range 1000 {
		key := []byte(fmt.Sprintf("key%d", i))
		if gc.Value(key, midpoint) == "old" {
			oldCount++
		} else {
			newCount++
		}
	}
	assert.Greater(t, oldCount, 300, "should have some keys still on old value")
	assert.Greater(t, newCount, 300, "should have some keys on new value")
}

func TestGradualChangeValue_TransitionProgresses(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}

	early := start.Add(time.Hour)
	late := end.Add(-time.Hour)

	earlyNew, lateNew := 0, 0
	for i := range 1000 {
		key := []byte(fmt.Sprintf("key%d", i))
		if gc.Value(key, early) == "new" {
			earlyNew++
		}
		if gc.Value(key, late) == "new" {
			lateNew++
		}
	}
	assert.Greater(t, lateNew, earlyNew, "more keys should be new later in the transition")
}

func TestGradualChangeValue_StartEndEqual(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}
	key := []byte("key")

	assert.Equal(t, "old", gc.Value(key, start.Add(-time.Hour)))
	assert.Equal(t, "new", gc.Value(key, start))
	assert.Equal(t, "new", gc.Value(key, start.Add(time.Hour)))
}

func TestGradualChangeValue_Monotonic(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}

	for i := range 1000 {
		key := []byte(fmt.Sprintf("key%d", i))
		earlyNew := gc.Value(key, start.Add(24*time.Hour)) == "new"
		midNew := gc.Value(key, start.Add(5*24*time.Hour)) == "new"
		lateNew := gc.Value(key, end.Add(-24*time.Hour)) == "new"
		if earlyNew {
			assert.True(t, midNew && lateNew, "key should not revert from new to old")
		} else if midNew {
			assert.True(t, lateNew, "key should not revert from new to old")
		}
	}
}

func TestGradualChangeWhen_DifferentKeysHaveDifferentTimes(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[bool]{Old: false, New: true, Start: start, End: end}

	when1 := gc.When([]byte("key1"))
	when2 := gc.When([]byte("key2"))
	when3 := gc.When([]byte("different_key"))

	assert.True(t, !when1.Equal(when2) || !when2.Equal(when3),
		"different keys should generally have different transition times")
}

func TestGradualChangeWhen_ConsistentForSameKey(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	gc := GradualChange[bool]{Old: false, New: true, Start: start, End: end}

	key := []byte("test_key")
	when1 := gc.When(key)
	when2 := gc.When(key)
	assert.Equal(t, when1, when2, "same key should always return same time")
}

func TestStaticGradualChange_AlwaysReturnsValue(t *testing.T) {
	gc := StaticGradualChange(42)

	times := []time.Time{
		time.Now().Add(-time.Hour * 24 * 365),
		time.Now(),
		time.Now().Add(time.Hour * 24 * 365),
	}

	for _, tm := range times {
		for _, key := range []string{"key1", "key2", ""} {
			assert.Equal(t, 42, gc.Value([]byte(key), tm))
		}
	}
}

func TestStaticGradualChange_WhenReturnsZeroOrPast(t *testing.T) {
	gc := StaticGradualChange(42)
	when := gc.When([]byte("any_key"))
	assert.True(t, when.IsZero() || when.Before(time.Now()))
}

func TestConvertGradualChange_Bool(t *testing.T) {
	conv := ConvertGradualChange(false)

	gc, err := conv(true)
	require.NoError(t, err)
	assert.True(t, gc.New)
}

func TestConvertGradualChange_BoolStruct(t *testing.T) {
	conv := ConvertGradualChange(false)

	gc, err := conv(map[string]any{
		"Old":   false,
		"New":   true,
		"Start": "2024-01-01T00:00:00Z",
		"End":   "2024-01-10T00:00:00Z",
	})
	require.NoError(t, err)
	assert.False(t, gc.Old)
	assert.True(t, gc.New)
	assert.True(t, gc.Start.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	assert.True(t, gc.End.Equal(time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)))
}

func TestConvertGradualChange_Int(t *testing.T) {
	conv := ConvertGradualChange(0)

	gc, err := conv(42)
	require.NoError(t, err)
	assert.Equal(t, 42, gc.New)
}

func TestConvertGradualChange_IntStruct(t *testing.T) {
	conv := ConvertGradualChange(0)

	gc, err := conv(map[string]any{"New": 100, "Old": 50})
	require.NoError(t, err)
	assert.Equal(t, 100, gc.New)
	assert.Equal(t, 50, gc.Old)
}

func TestConvertGradualChange_Float64(t *testing.T) {
	conv := ConvertGradualChange(0.0)

	gc, err := conv(3.14)
	require.NoError(t, err)
	assert.InEpsilon(t, 3.14, gc.New, 1e-10)
}

func TestConvertGradualChange_String(t *testing.T) {
	conv := ConvertGradualChange("")

	gc, err := conv("hello")
	require.NoError(t, err)
	assert.Equal(t, "hello", gc.New)
}

func TestConvertGradualChange_DurationString(t *testing.T) {
	conv := ConvertGradualChange(time.Duration(0))

	gc, err := conv("5m")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, gc.New)
}

func TestConvertGradualChange_DurationNumeric(t *testing.T) {
	conv := ConvertGradualChange(time.Duration(0))

	gc, err := conv(30)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, gc.New)
}

func TestSubscribeGradualChange_InitialValues(t *testing.T) {
	ts := clock.NewEventTimeSource()

	start := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	ts.Update(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}

	subscribable := func(cb func(GradualChange[string])) (GradualChange[string], func()) {
		return gc, func() {}
	}

	var calls []string
	cb := func(v string) { calls = append(calls, v) }
	initial, cancel := SubscribeGradualChange(subscribable, []byte("key"), cb, ts)
	cancel()

	assert.Equal(t, "old", initial)
	assert.Empty(t, calls)

	ts.Update(time.Date(2024, 1, 30, 0, 0, 0, 0, time.UTC))

	initial, cancel = SubscribeGradualChange(subscribable, []byte("key"), cb, ts)
	cancel()

	assert.Equal(t, "new", initial)
	assert.Empty(t, calls)
}

func TestSubscribeGradualChange_CallbackOnConfigChange(t *testing.T) {
	ts := clock.NewEventTimeSource()
	ts.Update(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC))

	gc := GradualChange[int]{New: 100}
	var changeCallback func(GradualChange[int])

	subscribable := func(cb func(GradualChange[int])) (GradualChange[int], func()) {
		changeCallback = cb
		return gc, func() {}
	}

	var callbackVals []int
	initial, cancel := SubscribeGradualChange(subscribable, []byte("key"), func(v int) {
		callbackVals = append(callbackVals, v)
	}, ts)
	defer cancel()

	assert.Equal(t, 100, initial)
	assert.Empty(t, callbackVals)

	changeCallback(GradualChange[int]{New: 200})
	assert.Equal(t, []int{200}, callbackVals)

	changeCallback(GradualChange[int]{New: 300})
	assert.Equal(t, []int{200, 300}, callbackVals)

	changeCallback(GradualChange[int]{New: 300})
	assert.Equal(t, []int{200, 300}, callbackVals, "should not be called for same value")
}

func TestSubscribeGradualChange_TimerFiresAtTransitionTime(t *testing.T) {
	ts := clock.NewEventTimeSource()
	ts.UseAsyncTimers(true)

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)
	ts.Update(start.Add(-time.Hour))

	gc := GradualChange[bool]{Old: false, New: true, Start: start, End: end}

	subscribable := func(cb func(GradualChange[bool])) (GradualChange[bool], func()) {
		return gc, func() {}
	}

	key := []byte("test_key")
	var callbackVals syncSlice[bool]
	initial, cancel := SubscribeGradualChange(subscribable, key, func(v bool) {
		callbackVals.append(v)
	}, ts)
	defer cancel()

	assert.False(t, initial)

	ts.Update(gc.When(key).Add(-time.Second))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Empty(c, callbackVals.get())
	}, time.Second, time.Millisecond)

	ts.Update(gc.When(key).Add(time.Second))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(t, []bool{true}, callbackVals.get())
	}, time.Second, time.Millisecond)

	ts.Update(end.Add(time.Second))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(t, []bool{true}, callbackVals.get())
	}, time.Second, time.Millisecond)
}

func TestSubscribeGradualChange_CancelStopsUpdates(t *testing.T) {
	ts := clock.NewEventTimeSource()
	ts.UseAsyncTimers(true)

	start := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	ts.Update(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	gc := GradualChange[string]{Old: "old", New: "new", Start: start, End: end}
	var subCancelCalled atomic.Bool
	subscribable := func(cb func(GradualChange[string])) (GradualChange[string], func()) {
		return gc, func() { subCancelCalled.Store(true) }
	}

	var callbackVals syncSlice[string]
	_, cancel := SubscribeGradualChange(subscribable, []byte("key"), func(v string) {
		callbackVals.append(v)
	}, ts)

	cancel()
	assert.True(t, subCancelCalled.Load())

	ts.Update(end.Add(time.Hour))
	time.Sleep(10 * time.Millisecond) // nolint:forbidigo // check for lack of async call
	assert.Empty(t, callbackVals.get(), "callback should not be called after cancel")
}

func TestSubscribeGradualChange_ConstantValueNoTimer(t *testing.T) {
	ts := clock.NewEventTimeSource()
	ts.Update(time.Now())

	gc := StaticGradualChange("constant")

	subscribable := func(cb func(GradualChange[string])) (GradualChange[string], func()) {
		return gc, func() {}
	}

	initial, cancel := SubscribeGradualChange(subscribable, []byte("key"), func(v string) {
		t.Error("callback should not be called for constant value")
	}, ts)
	defer cancel()

	assert.Equal(t, "constant", initial)
	assert.Equal(t, 0, ts.NumTimers())
}

type syncSlice[T any] struct {
	lock sync.Mutex
	s    []T
}

func (ss *syncSlice[T]) append(v T) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	ss.s = append(ss.s, v)
}

func (ss *syncSlice[T]) get() []T {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	return slices.Clone(ss.s)
}

package testlogger

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
)

type testStringer struct{}

func (testStringer) String() string { return "stringer" }

// component mimics a live service object that is mutated concurrently with logging.
type component struct {
	pollers map[int]string // mutated without synchronization; formatValue must never read it
}

func TestFormatValue_PreservesBasicTypes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, "<nil>"},
		{"string", "hello", "hello"},
		{"stringer", testStringer{}, "stringer"},
		{"duration", 1500 * time.Millisecond, "1.5s"},
		{"error", errors.New("boom"), "boom"},
		{"int", 42, "42"},
		{"int64", int64(-7), "-7"},
		{"uint32", uint32(7), "7"},
		{"float", 1.5, "1.5"},
		{"bool", true, "true"},
		{"string slice", []string{"a", "b"}, "[a b]"},
		{"empty slice", []int{}, "[]"},
		{"any slice", []any{1, "x", errors.New("e")}, "[1 x e]"},
		{"bytes", []byte{1, 2}, "[1 2]"},
		{"nil pointer", (*component)(nil), "<nil>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, formatAny(tc.value))
		})
	}
}

func TestFormatValue_TagValue(t *testing.T) {
	t.Parallel()

	require.Equal(t, "abc", formatValue(tag.NewStringTag("k", "abc")))
	require.Equal(t, "5", formatValue(tag.NewInt("k", 5)))
	require.Equal(t, "[a b]", formatValue(tag.NewStringsTag("k", []string{"a", "b"})))
}

func TestFormatValue_DoesNotDeepWalkCompositeValues(t *testing.T) {
	t.Parallel()

	c := &component{pollers: map[int]string{1: "secret-poller"}}
	type wrapper struct{ C *component }

	got := formatValue(tag.Any("key", c))
	require.Equal(t, fmt.Sprintf("*testlogger.component(%p)", c), got)
	require.NotContains(t, got, "secret-poller")

	require.Equal(t, "testlogger.wrapper", formatAny(wrapper{C: c}))
	require.Equal(t, "map[int]string", formatAny(c.pollers))
	require.Equal(t, "func()", formatAny(func() {}))
}

// TestFormatValue_NoRaceWithConcurrentMutation fails under -race if formatValue reads the
// contents of a value that another goroutine is mutating without synchronization.
func TestFormatValue_NoRaceWithConcurrentMutation(t *testing.T) {
	t.Parallel()

	c := &component{pollers: map[int]string{}}
	tags := []tag.Tag{
		tag.Any("ptr", c),
		tag.Any("struct", *c), // copy shares the map
		tag.Any("map", c.pollers),
		tag.Any("slice", []*component{c}),
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			c.pollers[i] = "poller" // intentionally unsynchronized, like the matching engine's pollers map
		}
	})
	wg.Go(func() {
		for range 1000 {
			for _, tg := range tags {
				_ = formatValue(tg)
			}
		}
	})
	wg.Wait()
}

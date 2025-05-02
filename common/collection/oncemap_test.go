package collection

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnceMap(t *testing.T) {
	counter := atomic.Int32{}
	m := NewOnceMap(func(k string) int {
		return int(counter.Add(1))
	})

	foo1 := m.Get("foo")
	foo2 := m.Get("foo")

	require.Equal(t, 1, foo1)
	require.Equal(t, 1, foo2)

	bar1 := m.Get("bar")
	bar2 := m.Get("bar")

	require.Equal(t, 2, bar1)
	require.Equal(t, 2, bar2)
}

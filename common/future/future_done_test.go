package future

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDone(t *testing.T) {
	t.Parallel()
	f := NewFuture[int]()
	select {
	case <-f.Done():
		t.Fatal("pending future is complete")
	default:
	}
	f.Set(42, nil)
	select {
	case <-f.Done():
	default:
		t.Fatal("completed future did not notify")
	}
	value, err := f.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, 42, value)
}

package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	cache := NewSimple(nil)

	cache.Put("A", "Foo")
	require.Equal(t, "Foo", cache.Get("A"))
	require.Nil(t, cache.Get("B"))
	require.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	require.Equal(t, 4, cache.Size())

	require.Equal(t, "Bar", cache.Get("B"))
	require.Equal(t, "Cid", cache.Get("C"))
	require.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	require.Equal(t, "Foo2", cache.Get("A"))

	cache.Put("E", "Epsi")
	require.Equal(t, "Epsi", cache.Get("E"))
	require.Equal(t, "Foo2", cache.Get("A"))

	cache.Delete("A")
	require.Nil(t, cache.Get("A"))
}

func TestSimpleGenerics(t *testing.T) {
	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := NewSimple(nil)
	cache.Put(key, value)

	require.Equal(t, value, cache.Get(key))
	require.Equal(t, value, cache.Get(keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	require.Nil(t, cache.Get(keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
}

func TestSimpleCacheConcurrentAccess(t *testing.T) {
	cache := NewSimple(nil)
	values := map[string]string{
		"A": "foo",
		"B": "bar",
		"C": "zed",
		"D": "dank",
		"E": "ezpz",
	}

	for k, v := range values {
		cache.Put(k, v)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 20 {
		// concurrent get and put
		wg.Go(func() {
			<-start

			for range 1000 {
				cache.Get("A")
				cache.Put("A", "fooo")
			}
		})

		// concurrent iteration
		wg.Go(func() {
			<-start

			for range 50 {
				it := cache.Iterator()
				for it.HasNext() {
					_ = it.Next()
				}
				it.Close()
			}
		})
	}

	close(start)
	wg.Wait()
}

func TestSimpleRemoveFunc(t *testing.T) {
	ch := make(chan bool)
	cache := NewSimple(&SimpleOptions{
		RemovedFunc: func(i any) {
			_, ok := i.(*testing.T)
			ch <- ok
		},
	})

	cache.Put("testing", t)
	cache.Delete("testing")
	require.Nil(t, cache.Get("testing"))

	timeout := time.NewTimer(time.Millisecond * 300)
	select {
	case b := <-ch:
		require.True(t, b)
	case <-timeout.C:
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestSimpleIterator(t *testing.T) {
	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := NewSimple(nil)

	for k, v := range expected {
		cache.Put(k, v)
	}

	actual := map[string]string{}

	it := cache.Iterator()
	for it.HasNext() {
		entry := it.Next()
		// nolint:revive
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	require.Equal(t, expected, actual)

	it = cache.Iterator()
	for i := 0; i < len(expected); i++ {
		entry := it.Next()
		// nolint:revive
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	require.Equal(t, expected, actual)
}

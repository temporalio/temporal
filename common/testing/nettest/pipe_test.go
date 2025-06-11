package nettest_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/testing/nettest"
)

func TestPipe_Accept(t *testing.T) {
	t.Parallel()

	pipe := nettest.NewPipe()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)

	go func() {
		defer wg.Done()

		c, err := pipe.Accept(nil)
		assert.NoError(t, err)

		defer func() {
			assert.NoError(t, c.Close())
		}()
	}()

	c, err := pipe.Connect(nil)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func TestPipe_ClientCanceled(t *testing.T) {
	t.Parallel()

	pipe := nettest.NewPipe()
	done := make(chan struct{})
	close(done) // hi efe
	_, err := pipe.Connect(done)
	assert.ErrorIs(t, err, nettest.ErrCanceled)
}

func TestPipe_ServerCanceled(t *testing.T) {
	t.Parallel()

	pipe := nettest.NewPipe()
	done := make(chan struct{})
	close(done)
	_, err := pipe.Accept(done)
	assert.ErrorIs(t, err, nettest.ErrCanceled)
}

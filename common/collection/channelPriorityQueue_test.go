package collection

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChannelPriorityQueue(t *testing.T) {
	queue := NewChannelPriorityQueue(1)

	shutdown := queue.Add(1, 20)
	assert.True(t, shutdown)

	shutdown = queue.Add(0, 10)
	assert.True(t, shutdown)

	item, ok := queue.Remove()
	assert.Equal(t, 10, item)
	assert.True(t, ok)

	item, ok = queue.Remove()
	assert.Equal(t, 20, item)
	assert.True(t, ok)

	queue.Close()

	// once we close the channel we should get shutdown at least once
	for {
		shutdown = queue.Add(1, 20)
		if !shutdown {
			break
		}
	}

	item, ok = queue.Remove()
	assert.Nil(t, item)
	assert.False(t, ok)
}

func BenchmarkChannelPriorityQueue(b *testing.B) {
	queue := NewChannelPriorityQueue(100)

	for i := 0; i < 10; i++ {
		go sendChannelQueue(queue)
	}

	for n := 0; n < b.N; n++ {
		queue.Remove()
	}
}

func sendChannelQueue(queue ChannelPriorityQueue) {
	for {
		priority := rand.Int() % numPriorities
		queue.Add(priority, struct{}{})
	}
}

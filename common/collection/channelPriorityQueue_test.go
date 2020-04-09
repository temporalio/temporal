// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

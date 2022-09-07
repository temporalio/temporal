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

package tasks

const (
	WeightedChannelDefaultSize = 1000
)

type (
	WeightedChannels[T Task] []*WeightedChannel[T]

	WeightedChannel[T Task] struct {
		weight  int
		channel chan T
	}
)

func NewWeightedChannel[T Task](
	weight int,
	size int,
) *WeightedChannel[T] {
	return &WeightedChannel[T]{
		weight:  weight,
		channel: make(chan T, size),
	}
}

func (c *WeightedChannel[T]) Chan() chan T {
	return c.channel
}

func (c *WeightedChannel[T]) Weight() int {
	return c.weight
}

func (c *WeightedChannel[T]) SetWeight(newWeight int) {
	c.weight = newWeight
}

func (c *WeightedChannel[T]) Len() int {
	return len(c.channel)
}

func (c *WeightedChannel[T]) Cap() int {
	return cap(c.channel)
}

func (c WeightedChannels[T]) Len() int {
	return len(c)
}
func (c WeightedChannels[T]) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c WeightedChannels[T]) Less(i, j int) bool {
	return c[i].Weight() < c[j].Weight()
}

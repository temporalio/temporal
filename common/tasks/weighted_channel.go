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
	WeightedChannelDefaultSize = 10000
)

type (
	WeightedChannels []*WeightedChannel

	WeightedChannel struct {
		weight  int
		channel chan PriorityTask
	}
)

func NewWeightedChannel(
	weight int,
	size int,
) *WeightedChannel {
	return &WeightedChannel{
		weight:  weight,
		channel: make(chan PriorityTask, size),
	}
}

func (c *WeightedChannel) Chan() chan PriorityTask {
	return c.channel
}

func (c *WeightedChannel) Weight() int {
	return c.weight
}

func (c *WeightedChannel) Len() int {
	return len(c.channel)
}

func (c *WeightedChannel) Cap() int {
	return cap(c.channel)
}

func (c WeightedChannels) Len() int {
	return len(c)
}
func (c WeightedChannels) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c WeightedChannels) Less(i, j int) bool {
	return c[i].Weight() < c[j].Weight()
}

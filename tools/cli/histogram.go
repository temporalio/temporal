// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	bar          = "*"
	defaultWidth = 100
)

type counter struct {
	key   string
	count int
}

// Histogram holds the occurrence count for each key
type Histogram struct {
	maxCount int    // used to format output
	maxKey   string // used to format output

	counters []*counter
}

// NewHistogram creates a new Histogram
func NewHistogram() *Histogram {
	return &Histogram{
		maxCount: defaultWidth,
		maxKey:   "Bucket",
	}
}

// Add will increment occurrence count of the key
func (h *Histogram) Add(key string) {
	var found bool
	for _, c := range h.counters {
		if c.key == key {
			found = true
			c.count++
			if c.count > h.maxCount {
				h.maxCount = c.count
			}
			break
		}
	}

	if !found {
		h.counters = append(h.counters, &counter{
			key:   key,
			count: 1,
		})
	}

	if len(key) > len(h.maxKey) {
		h.maxKey = key
	}
}

func (h *Histogram) addMultiplier(multiplier int) {
	for _, counter := range h.counters {
		counter.count = counter.count * multiplier
	}

	h.maxCount = h.maxCount * multiplier
}

// Print will output histogram with key and counter information.
func (h *Histogram) Print(multiplier int) error {
	h.addMultiplier(multiplier)
	sort.Sort(h)

	keyLength := len(h.maxKey)
	countLength := len(strconv.FormatInt(int64(h.maxCount), 10))

	fmt.Printf("%-*s %*s\n", keyLength, "Bucket", countLength, "Count")

	for _, c := range h.counters {
		w := (defaultWidth - keyLength - countLength) * c.count / h.maxCount
		fmt.Printf("%-*s %*d %s\n", keyLength, c.key, countLength, c.count, strings.Repeat(bar, w))
	}

	return nil
}

func (h *Histogram) Len() int { return len(h.counters) }

func (h *Histogram) Less(i, j int) bool {
	return h.counters[i].key < h.counters[j].key
}

func (h *Histogram) Swap(i, j int) {
	h.counters[j], h.counters[i] = h.counters[i], h.counters[j]
}

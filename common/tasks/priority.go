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

import "strconv"

type (
	Priority int
)

const (
	numBitsPerLevel = 3
)

const (
	highPriorityClass Priority = iota << numBitsPerLevel
	mediumPriorityClass
	lowPriorityClass
)

const (
	highPrioritySubclass Priority = iota
	mediumPrioritySubclass
	lowPrioritySubclass
)

var (
	PriorityHigh   = getPriority(highPriorityClass, mediumPrioritySubclass)
	PriorityMedium = getPriority(mediumPriorityClass, mediumPrioritySubclass)
	PriorityLow    = getPriority(lowPriorityClass, mediumPrioritySubclass)
)

var (
	PriorityName = map[Priority]string{
		PriorityHigh:   "high",
		PriorityMedium: "medium",
		PriorityLow:    "low",
	}

	PriorityValue = map[string]Priority{
		"high":   PriorityHigh,
		"medium": PriorityMedium,
		"low":    PriorityLow,
	}
)

func (p Priority) String() string {
	s, ok := PriorityName[p]
	if ok {
		return s
	}
	return strconv.Itoa(int(p))
}

func getPriority(
	class, subClass Priority,
) Priority {
	return class | subClass
}

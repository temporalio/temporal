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

package configs

import "strconv"

const (
	numBitsPerLevel = 3
)

const (
	TaskHighPriorityClass = iota << numBitsPerLevel
	TaskDefaultPriorityClass
	TaskLowPriorityClass
)

const (
	TaskHighPrioritySubclass = iota
	TaskDefaultPrioritySubclass
	TaskLowPrioritySubclass
)

var DefaultTaskPriorityWeight = map[int]int{
	GetTaskPriority(TaskHighPriorityClass, TaskDefaultPrioritySubclass):    200,
	GetTaskPriority(TaskDefaultPriorityClass, TaskDefaultPrioritySubclass): 100,
	GetTaskPriority(TaskLowPriorityClass, TaskDefaultPrioritySubclass):     50,
}

func ConvertWeightsToDynamicConfigValue(
	weights map[int]int,
) map[string]interface{} {
	weightsForDC := make(map[string]interface{})
	for priority, weight := range weights {
		weightsForDC[strconv.Itoa(priority)] = weight
	}
	return weightsForDC
}

func GetTaskPriority(
	class, subClass int,
) int {
	return class | subClass
}

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

package task

const (
	numBitsPerLevel = 3
)

const (
	// NoPriority is the value returned if no priority is ever assigned to the task
	NoPriority = -1
)

const (
	// HighPriorityClass is the priority class for high priority tasks
	HighPriorityClass = iota << numBitsPerLevel
	// DefaultPriorityClass is the priority class for default priority tasks
	DefaultPriorityClass
	// LowPriorityClass is the priority class for low priority tasks
	LowPriorityClass
)

const (
	// HighPrioritySubclass is the priority subclass for high priority tasks
	HighPrioritySubclass = iota
	// DefaultPrioritySubclass is the priority subclass for high priority tasks
	DefaultPrioritySubclass
	// LowPrioritySubclass is the priority subclass for high priority tasks
	LowPrioritySubclass
)

// GetTaskPriority returns priority given a task's priority class and subclass
func GetTaskPriority(
	class int,
	subClass int,
) int {
	return class | subClass
}

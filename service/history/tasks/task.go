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

import (
	"time"
)

type (
	Key struct {
		// FireTime is the scheduled time of the task
		FireTime time.Time
		// TaskID is the ID of the task
		TaskID int64
	}

	Keys []Key

	// Task is the generic task interface
	Task interface {
		GetKey() Key
		GetNamespaceID() string
		GetWorkflowID() string
		GetRunID() string
		GetTaskID() int64
		GetVisibilityTime() time.Time
		GetVersion() int64

		SetVersion(version int64)
		SetTaskID(id int64)
		SetVisibilityTime(timestamp time.Time)
	}
)

func (left Key) CompareTo(right Key) int {
	if left.FireTime.Before(right.FireTime) {
		return -1
	} else if left.FireTime.After(right.FireTime) {
		return 1
	}

	if left.TaskID < right.TaskID {
		return -1
	} else if left.TaskID > right.TaskID {
		return 1
	}
	return 0
}

// Len implements sort.Interface
func (s Keys) Len() int {
	return len(s)
}

// Swap implements sort.Interface.
func (s Keys) Swap(
	this int,
	that int,
) {
	s[this], s[that] = s[that], s[this]
}

// Less implements sort.Interface
func (s Keys) Less(
	this int,
	that int,
) bool {
	return s[this].CompareTo(s[that]) < 0
}

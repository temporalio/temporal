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
	"fmt"
	"math"
	"time"
)

var (
	DefaultFireTime         = time.Unix(0, 0)
	defaultFireTimeUnixNano = DefaultFireTime.UnixNano()
)

var (
	MinimumKey = NewKey(DefaultFireTime, 0)
	MaximumKey = NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64)
)

type (
	Key struct {
		// FireTime is the scheduled time of the task
		FireTime time.Time
		// TaskID is the ID of the task
		TaskID int64
	}

	Keys []Key
)

func NewImmediateKey(taskID int64) Key {
	return Key{
		FireTime: DefaultFireTime,
		TaskID:   taskID,
	}
}

func NewKey(fireTime time.Time, taskID int64) Key {
	return Key{
		FireTime: fireTime,
		TaskID:   taskID,
	}
}

func ValidateKey(key Key) error {
	if key.FireTime.UnixNano() < defaultFireTimeUnixNano {
		return fmt.Errorf("task key fire time must have unix nano value >= 0, got %v", key.FireTime.UnixNano())
	}

	if key.TaskID < 0 {
		return fmt.Errorf("task key ID must >= 0, got %v", key.TaskID)
	}

	return nil
}

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

func (k Key) Prev() Key {
	if k.TaskID == 0 {
		if k.FireTime.UnixNano() == 0 {
			panic("Key encountered negative underflow")
		}
		return NewKey(k.FireTime.Add(-time.Nanosecond), math.MaxInt64)
	}
	return NewKey(k.FireTime, k.TaskID-1)
}

func (k Key) Next() Key {
	if k.TaskID == math.MaxInt64 {
		if k.FireTime.UnixNano() == math.MaxInt64 {
			panic("Key encountered positive overflow")
		}
		return NewKey(k.FireTime.Add(time.Nanosecond), 0)
	}
	return NewKey(k.FireTime, k.TaskID+1)
}

func MinKey(this Key, that Key) Key {
	if this.CompareTo(that) < 0 {
		return this
	}
	return that
}

func MaxKey(this Key, that Key) Key {
	if this.CompareTo(that) < 0 {
		return that
	}
	return this
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

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

package locks

import (
	"context"
)

type (
	PriorityMutex interface {
		// LockHigh try to lock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
		LockHigh(context.Context) error
		// LockLow try to lock with low priority, use LockLow / UnlockLow pair to lock / unlock
		LockLow(context.Context) error
		// UnlockHigh unlock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
		UnlockHigh()
		// UnlockLow unlock with low priority, use LockLow / UnlockLow pair to lock / unlock
		UnlockLow()
		// TryLockHigh try to lock with high priority, return true if lock acquired
		// use TryLockHigh / UnlockHigh pair to lock / unlock
		TryLockHigh() bool
		// TryLockLow try to lock with high priority, return true if lock acquired
		// use TryLockLow / UnlockLow pair to lock / unlock
		TryLockLow() bool
		// IsLocked returns true if already locked
		IsLocked() bool
	}
)

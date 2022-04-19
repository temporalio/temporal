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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination rescheduler_mock.go

package queues

import (
	"time"
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	// TODO: remove this component when implementing multi-cursor queue processor.
	// Failed task executables can be tracke by task reader/queue range
	Rescheduler interface {
		// Add task executable to the rescheudler.
		// The backoff duration is just a hint for how long the executable
		// should be bufferred before rescheduling.
		Add(task Executable, backoff time.Duration)

		// Reschedule re-submit buffered executables to the scheduler and stops when
		// targetRescheduleSize number of executables are successfully submitted.
		// If targetRescheduleSize is 0, then there's no limit for the number of reschduled
		// executables.
		Reschedule(targetRescheduleSize int)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
	}
)

package tasks

import (
	"time"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination scheduler_mock.go
type (
	// Scheduler is the generic interface for scheduling & processing tasks
	Scheduler[T Task] interface {
		Submit(task T)
		TrySubmit(task T) bool
		Start()
		Stop()
	}

	// SchedulerTimestampedTask is an optional interface that tasks can implement
	// to enable queue wait latency tracking in schedulers.
	SchedulerTimestampedTask interface {
		SetSchedulerEnqueueTime(time.Time)
		GetSchedulerEnqueueTime() time.Time
	}
)

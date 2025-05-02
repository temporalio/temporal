package queues

import (
	"go.temporal.io/server/service/history/tasks"
)

type (
	// Alert is created by a Monitor when some statistics of the Queue is abnormal
	Alert struct {
		AlertType                            AlertType
		AlertAttributesQueuePendingTaskCount *AlertAttributesQueuePendingTaskCount
		AlertAttributesReaderStuck           *AlertAttributesReaderStuck
		AlertAttributesSliceCount            *AlertAttributesSlicesCount
	}

	AlertType int

	AlertAttributesQueuePendingTaskCount struct {
		CurrentPendingTaskCount   int
		CiriticalPendingTaskCount int
	}

	AlertAttributesReaderStuck struct {
		ReaderID         int64
		CurrentWatermark tasks.Key
	}

	AlertAttributesSlicesCount struct {
		CurrentSliceCount  int
		CriticalSliceCount int
	}
)

const (
	AlertTypeUnspecified AlertType = iota
	AlertTypeQueuePendingTaskCount
	AlertTypeReaderStuck
	AlertTypeSliceCount
)

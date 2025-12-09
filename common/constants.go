package common

import (
	"math"
	"time"
)

const (
	// FirstEventID is the id of the first event in the history
	FirstEventID int64 = 1
	// LastEventID is the id of the last possible event in the history
	LastEventID int64 = math.MaxInt64
	// EmptyEventID is the id of the empty event
	EmptyEventID int64 = 0
	// EmptyVersion is used as the default value for failover version when no value is provided
	EmptyVersion int64 = 0
	// EndEventID is the id of the end event, here we use the int64 max
	EndEventID int64 = 1<<63 - 1
	// BufferedEventID is the id of the buffered event
	BufferedEventID int64 = -123
	// EmptyEventTaskID is uninitialized id of the task id within event
	EmptyEventTaskID int64 = 0
	// TransientEventID is the id of the transient event
	TransientEventID int64 = -124
	// FirstBlobPageToken is the page token identifying the first blob for each history archival
	FirstBlobPageToken = 1
	// LastBlobNextPageToken is the next page token on the last blob for each history archival
	LastBlobNextPageToken = -1
	// EndMessageID is the id of the end message, here we use the int64 max
	EndMessageID int64 = 1<<63 - 1
)

const (
	// MinLongPollTimeout is the minimum context timeout for long poll API, below which
	// the request won't be processed
	MinLongPollTimeout = time.Second * 2
	// CriticalLongPollTimeout is a threshold for the context timeout passed into long poll API,
	// below which a warning will be logged
	CriticalLongPollTimeout = time.Second * 10
)

const (
	// Limit for schedule notes field
	ScheduleNotesSizeLimit = 1000

	ScheduledTaskMinPrecision = time.Millisecond
)

const (
	// DefaultQueueReaderID is the default readerID when loading history tasks
	DefaultQueueReaderID int64 = 0
)

const (
	// DefaultOperatorRPSRatio is the default percentage of rate limit that should be used for operator priority requests
	DefaultOperatorRPSRatio float64 = 0.2
)

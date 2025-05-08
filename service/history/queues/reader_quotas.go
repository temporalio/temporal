package queues

import (
	"strconv"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"
)

const (
	readerRequestToken = 1
)

func NewReaderPriorityRateLimiter(
	rateFn quotas.RateFn,
	maxReaders int64,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter, maxReaders)
	readerCallerToPriority := make(map[string]int, maxReaders)
	for readerId := DefaultReaderId; readerId != DefaultReaderId+maxReaders; readerId++ {
		// use readerId as priority
		rateLimiters[int(readerId)] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(rateFn))
		// reader will use readerId (in string type) as caller when using the rate limiter
		readerCallerToPriority[newReaderRequest(readerId).Caller] = int(readerId)
	}
	lowestPriority := int(DefaultReaderId + maxReaders - 1)

	return quotas.NewPriorityRateLimiter(
		func(req quotas.Request) int {
			if priority, ok := readerCallerToPriority[req.Caller]; ok {
				return priority
			}
			return lowestPriority
		},
		rateLimiters,
	)
}

func newShardReaderRateLimiter(
	shardMaxPollRPS dynamicconfig.IntPropertyFn,
	hostReaderRateLimiter quotas.RequestRateLimiter,
	maxReaders int64,
) quotas.RequestRateLimiter {
	return quotas.NewMultiRequestRateLimiter(
		NewReaderPriorityRateLimiter(
			func() float64 { return float64(shardMaxPollRPS()) },
			maxReaders,
		),
		hostReaderRateLimiter,
	)
}

func newReaderRequest(
	readerID int64,
) quotas.Request {
	// The priority is only based on readerID (caller),
	// api, caller type, caller segment, and call initiation (origin)
	// are the same for all the readers, and not related to
	// priority so leaving those fields empty.
	return quotas.NewRequest(
		"",
		readerRequestToken,
		strconv.FormatInt(readerID, 10),
		"",
		0,
		"",
	)
}

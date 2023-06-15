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

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

const (
	defaultReaderPriority    = 0
	nonDefaultReaderPriority = defaultReaderPriority + 1
)

var (
	readerPriorities = []int{
		defaultReaderPriority,
		nonDefaultReaderPriority,
	}
)

var (
	defaultReaderCaller = newReaderRequest(defaultReaderId).Caller
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range readerPriorities {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(rateFn)
	}

	return quotas.NewPriorityRateLimiter(
		func(req quotas.Request) int {
			if req.Caller == defaultReaderCaller {
				return defaultReaderPriority
			}
			return nonDefaultReaderPriority
		},
		rateLimiters,
	)
}

func newReaderRateLimiter(
	shardMaxPollRPS dynamicconfig.IntPropertyFn,
	hostRateLimiter quotas.RequestRateLimiter,
) quotas.RequestRateLimiter {
	return quotas.NewMultiRequestRateLimiter(
		NewPriorityRateLimiter(func() float64 {
			return float64(shardMaxPollRPS())
		}),
		hostRateLimiter,
	)
}

func newReaderRequest(
	readerID int,
) quotas.Request {
	return quotas.NewRequest(
		"",
		readerRequestToken,
		strconv.Itoa(readerID),
		"",
		"",
	)
}

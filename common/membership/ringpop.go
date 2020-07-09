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

package membership

import (
	"sync/atomic"
	"time"

	"github.com/temporalio/ringpop-go/discovery/statichosts"

	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/swim"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	// Number of times we retry refreshing the bootstrap list and try to join the Ringpop cluster before giving up
	maxBootstrapRetries = 5
)

type (
	// RingPop is a simple wrapper
	RingPop struct {
		status int32
		*ringpop.Ringpop
		logger          log.Logger
		maxJoinDuration time.Duration
	}
)

// NewRingPop create a new ring pop wrapper
func NewRingPop(
	ringPop *ringpop.Ringpop,
	maxJoinDuration time.Duration,
	logger log.Logger,
) *RingPop {
	return &RingPop{
		status:          common.DaemonStatusInitialized,
		Ringpop:         ringPop,
		maxJoinDuration: maxJoinDuration,
		logger:          logger,
	}
}

// Start start ring pop
func (r *RingPop) Start(
	bootstrapHostPostRetriever func() ([]string, error),
	bootstrapRetryBackoffInterval time.Duration,
) {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.bootstrap(bootstrapHostPostRetriever, bootstrapRetryBackoffInterval)
}

func (r *RingPop) bootstrap(
	bootstrapHostPostRetriever func() ([]string, error),
	bootstrapRetryBackoffInterval time.Duration,
) {
	retryCount := 0

	for {
		hostPorts, err := bootstrapHostPostRetriever()
		if err != nil {
			r.logger.Fatal("unable to bootstrap ringpop. unable to read hostport bootstrap list", tag.Error(err))
		}

		bootParams := &swim.BootstrapOptions{
			ParallelismFactor: 10,
			JoinSize:          1,
			MaxJoinDuration:   r.maxJoinDuration,
			DiscoverProvider:  statichosts.New(hostPorts...),
		}

		_, err = r.Ringpop.Bootstrap(bootParams)
		if err == nil {
			return
		}

		if retryCount >= maxBootstrapRetries {
			r.logger.Fatal("unable to bootstrap ringpop. exhausted all retries", tag.Error(err))
		}

		r.logger.Error("unable to bootstrap ringpop. retrying", tag.Error(err))
		retryCount = retryCount + 1
		time.Sleep(bootstrapRetryBackoffInterval)
	}
}

// Stop stop ring pop
func (r *RingPop) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.Destroy()
}

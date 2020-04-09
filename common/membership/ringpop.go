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

	"github.com/uber/ringpop-go/discovery/statichosts"

	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/swim"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
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
func (r *RingPop) Start(bootstrapHostPorts []string) {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	bootParams := &swim.BootstrapOptions{
		MaxJoinDuration:  r.maxJoinDuration,
		DiscoverProvider: statichosts.New(bootstrapHostPorts...),
	}

	_, err := r.Ringpop.Bootstrap(bootParams)
	if err != nil {
		r.logger.Fatal("unable to bootstrap ringpop", tag.Error(err))
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

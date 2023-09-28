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

package queues_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

type (
	testWrapper       struct{}
	wrappedExecutable struct {
		queues.Executable
	}
)

func TestNewExecutableFactoryWrapper(t *testing.T) {
	t.Parallel()

	wrapper := testWrapper{}
	factory := queues.NewExecutableFactory(
		nil,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		clock.NewEventTimeSource(),
		nil,
		nil,
		log.NewNoopLogger(),
		nil,
	)
	wrappedFactory := queues.NewExecutableFactoryWrapper(factory, wrapper)
	executable := wrappedFactory.NewExecutable(&tasks.WorkflowTask{}, 0)
	_, ok := executable.(wrappedExecutable)
	assert.True(t, ok, "expected executable to be wrapped")
}

func (t testWrapper) Wrap(e queues.Executable) queues.Executable {
	return wrappedExecutable{e}
}

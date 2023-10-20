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

package history_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	fakeMetadata struct {
		cluster.Metadata
	}
)

var errTerminal = new(serialization.DeserializationError)

func TestNewExecutableDLQWrapper(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		enableDLQ bool
	}{
		{
			name:      "DLQEnabled",
			enableDLQ: true,
		},
		{
			name:      "DLQDisabled",
			enableDLQ: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			fakeQueueWriter := &queuestest.FakeQueueWriter{}
			var w queues.ExecutableWrapper
			fxtest.New(
				t,
				fx.Provide(
					func() *queues.DLQWriter {
						return queues.NewDLQWriter(fakeQueueWriter)
					},
					func() *configs.Config {
						dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient(map[dynamicconfig.Key]interface{}{
							dynamicconfig.HistoryTaskDLQEnabled: tc.enableDLQ,
						}), log.NewTestLogger())
						return configs.NewConfig(dc, 1, false, false)
					},
					func() cluster.Metadata {
						return fakeMetadata{}
					},
					func() namespace.Registry {
						registry := namespace.NewMockRegistry(ctrl)
						registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).
							AnyTimes()
						return registry
					},
					func() clock.TimeSource {
						return clock.NewEventTimeSource()
					},
					func() log.Logger {
						return log.NewTestLogger()
					},
					func() metrics.Handler {
						return metrics.NoopMetricsHandler
					},
					fx.Annotate(history.NewExecutableDLQWrapper, fx.As(new(queues.ExecutableWrapper))),
				),
				fx.Populate(&w),
			)
			executable := w.Wrap(queuestest.NewFakeExecutable(&tasks.WorkflowTask{}, errTerminal))

			err := executable.Execute()
			if tc.enableDLQ {
				assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
			} else {
				assert.NotErrorIs(t, err, queues.ErrTerminalTaskFailure)
				assert.ErrorIs(t, err, errTerminal)
			}
			err = executable.Execute()
			if tc.enableDLQ {
				assert.NoError(t, err)
				assert.Len(t, fakeQueueWriter.EnqueueTaskRequests, 1)
			} else {
				assert.ErrorIs(t, err, errTerminal)
				assert.Empty(t, fakeQueueWriter.EnqueueTaskRequests)
			}
		})
	}
}

func (f fakeMetadata) GetCurrentClusterName() string {
	return "test-cluster-name"
}

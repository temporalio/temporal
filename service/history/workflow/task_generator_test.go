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

//
// The MIT License
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

package workflow

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func TestTaskGeneratorImpl_GenerateWorkflowCloseTasks(t *testing.T) {
	for _, c := range []struct {
		Name                               string
		DurableArchivalEnabled             bool
		DeleteAfterClose                   bool
		ExpectCloseExecutionVisibilityTask bool
		ExpectArchiveExecutionTask         bool
		ExpectDeleteHistoryEventTask       bool
	}{
		{
			Name:                   "Delete after retention",
			DurableArchivalEnabled: false,
			DeleteAfterClose:       false,

			ExpectCloseExecutionVisibilityTask: true,
			ExpectDeleteHistoryEventTask:       true,
			ExpectArchiveExecutionTask:         false,
		},
		{
			Name:                   "Use archival queue",
			DurableArchivalEnabled: true,
			DeleteAfterClose:       false,

			ExpectCloseExecutionVisibilityTask: true,
			ExpectDeleteHistoryEventTask:       false,
			ExpectArchiveExecutionTask:         true,
		},
		{
			Name:                   "DeleteAfterClose",
			DurableArchivalEnabled: false,
			DeleteAfterClose:       true,

			ExpectCloseExecutionVisibilityTask: false,
			ExpectDeleteHistoryEventTask:       false,
			ExpectArchiveExecutionTask:         false,
		},
		{
			Name:                   "DeleteAfterClose ignores durable execution flag",
			DurableArchivalEnabled: true,
			DeleteAfterClose:       true,

			ExpectCloseExecutionVisibilityTask: false,
			ExpectDeleteHistoryEventTask:       false,
			ExpectArchiveExecutionTask:         false,
		},
	} {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			retention := 24 * time.Hour
			namespaceEntry := tests.GlobalNamespaceEntry.Clone(namespace.WithRetention(&retention))
			namespaceRegistry.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceEntry.ID(), nil).AnyTimes()
			namespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()

			mutableState := NewMockMutableState(ctrl)
			mutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
			mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
				NamespaceId: namespaceEntry.ID().String(),
			}).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				namespaceEntry.ID().String(), tests.WorkflowID, tests.RunID,
			)).AnyTimes()
			mutableState.EXPECT().GetCurrentBranchToken().Return(nil, nil)
			taskGenerator := NewTaskGenerator(namespaceRegistry, mutableState, &configs.Config{
				DurableArchivalEnabled: func() bool {
					return c.DurableArchivalEnabled
				},
				RetentionTimerJitterDuration: func() time.Duration {
					return time.Second
				},
			})

			closeTime := time.Unix(0, 0)

			mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
				var (
					closeExecutionTask           *tasks.CloseExecutionTask
					deleteHistoryEventTask       *tasks.DeleteHistoryEventTask
					closeExecutionVisibilityTask *tasks.CloseExecutionVisibilityTask
					archiveExecutionTask         *tasks.ArchiveExecutionTask
				)
				for _, task := range ts {
					switch t := task.(type) {
					case *tasks.CloseExecutionTask:
						closeExecutionTask = t
					case *tasks.DeleteHistoryEventTask:
						deleteHistoryEventTask = t
					case *tasks.CloseExecutionVisibilityTask:
						closeExecutionVisibilityTask = t
					case *tasks.ArchiveExecutionTask:
						archiveExecutionTask = t
					}
				}
				require.NotNil(t, closeExecutionTask)
				assert.Equal(t, c.DeleteAfterClose, closeExecutionTask.DeleteAfterClose)

				if c.ExpectCloseExecutionVisibilityTask {
					assert.NotNil(t, closeExecutionVisibilityTask)
				} else {
					assert.Nil(t, closeExecutionVisibilityTask)
				}
				if c.ExpectArchiveExecutionTask {
					require.NotNil(t, archiveExecutionTask)
					assert.Equal(t, archiveExecutionTask.NamespaceID, namespaceEntry.ID().String())
					assert.Equal(t, archiveExecutionTask.WorkflowID, tests.WorkflowID)
					assert.Equal(t, archiveExecutionTask.RunID, tests.RunID)
				} else {
					assert.Nil(t, archiveExecutionTask)
				}
				if c.ExpectDeleteHistoryEventTask {
					require.NotNil(t, deleteHistoryEventTask)
					assert.Equal(t, deleteHistoryEventTask.NamespaceID, namespaceEntry.ID().String())
					assert.Equal(t, deleteHistoryEventTask.WorkflowID, tests.WorkflowID)
					assert.Equal(t, deleteHistoryEventTask.RunID, tests.RunID)
					assert.True(t, deleteHistoryEventTask.VisibilityTimestamp.After(closeTime.Add(retention)))
					assert.True(t, deleteHistoryEventTask.VisibilityTimestamp.Before(closeTime.Add(retention).Add(time.Second*2)))
				} else {
					assert.Nil(t, deleteHistoryEventTask)
				}
			})

			err := taskGenerator.GenerateWorkflowCloseTasks(&historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
				},
				EventTime: timestamp.TimePtr(closeTime),
			}, c.DeleteAfterClose)
			require.NoError(t, err)
		})
	}
}

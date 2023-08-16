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
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type testConfig struct {
	Name     string
	ConfigFn func(config *testParams)
}

type testParams struct {
	DeleteAfterClose                     bool
	CloseEventTime                       time.Time
	Retention                            time.Duration
	Logger                               *log.MockLogger
	ArchivalProcessorArchiveDelay        time.Duration
	HistoryArchivalEnabledInCluster      bool
	HistoryArchivalEnabledInNamespace    bool
	VisibilityArchivalEnabledForCluster  bool
	VisibilityArchivalEnabledInNamespace bool

	ExpectCloseExecutionVisibilityTask              bool
	ExpectArchiveExecutionTask                      bool
	ExpectDeleteHistoryEventTask                    bool
	ExpectedArchiveExecutionTaskVisibilityTimestamp time.Time
}

func TestTaskGeneratorImpl_GenerateWorkflowCloseTasks(t *testing.T) {
	for _, c := range []testConfig{
		{
			Name: "use archival queue",
			ConfigFn: func(p *testParams) {
				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "delete after close ignores durable execution flag",
			ConfigFn: func(p *testParams) {
				p.DeleteAfterClose = true
			},
		},
		{
			Name: "delay is zero",
			ConfigFn: func(p *testParams) {
				p.CloseEventTime = time.Unix(0, 0)
				p.Retention = 24 * time.Hour
				p.ArchivalProcessorArchiveDelay = 0

				p.ExpectedArchiveExecutionTaskVisibilityTimestamp = time.Unix(0, 0)
				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "delay exceeds retention",
			ConfigFn: func(p *testParams) {
				p.CloseEventTime = time.Unix(0, 0)
				p.Retention = 24 * time.Hour
				p.ArchivalProcessorArchiveDelay = 48*time.Hour + time.Second

				p.ExpectedArchiveExecutionTaskVisibilityTimestamp = time.Unix(0, 0).Add(24 * time.Hour)
				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "delay is less than retention",
			ConfigFn: func(p *testParams) {
				p.CloseEventTime = time.Unix(0, 0)
				p.Retention = 24 * time.Hour
				p.ArchivalProcessorArchiveDelay = 12 * time.Hour

				p.ExpectedArchiveExecutionTaskVisibilityTimestamp = time.Unix(0, 0).Add(p.ArchivalProcessorArchiveDelay)
				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "history archival disabled",
			ConfigFn: func(p *testParams) {
				p.HistoryArchivalEnabledInCluster = false
				p.HistoryArchivalEnabledInNamespace = false

				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "visibility archival disabled",
			ConfigFn: func(p *testParams) {
				p.VisibilityArchivalEnabledForCluster = false
				p.VisibilityArchivalEnabledInNamespace = false

				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "archival disabled in cluster",
			ConfigFn: func(p *testParams) {
				p.HistoryArchivalEnabledInCluster = false
				p.VisibilityArchivalEnabledForCluster = false

				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectDeleteHistoryEventTask = true
				p.ExpectArchiveExecutionTask = false
			},
		},
		{
			Name: "archival disabled in namespace",
			ConfigFn: func(p *testParams) {
				p.HistoryArchivalEnabledInNamespace = false
				p.VisibilityArchivalEnabledInNamespace = false

				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectDeleteHistoryEventTask = true
				p.ExpectArchiveExecutionTask = false
			},
		},
	} {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			now := time.Unix(0, 0).UTC()
			ctrl := gomock.NewController(t)
			mockLogger := log.NewMockLogger(ctrl)
			p := testParams{
				DeleteAfterClose:                     false,
				CloseEventTime:                       now,
				Retention:                            time.Hour * 24 * 7,
				Logger:                               mockLogger,
				HistoryArchivalEnabledInCluster:      true,
				HistoryArchivalEnabledInNamespace:    true,
				VisibilityArchivalEnabledForCluster:  true,
				VisibilityArchivalEnabledInNamespace: true,

				ExpectCloseExecutionVisibilityTask:              false,
				ExpectArchiveExecutionTask:                      false,
				ExpectDeleteHistoryEventTask:                    false,
				ExpectedArchiveExecutionTaskVisibilityTimestamp: now,
			}
			c.ConfigFn(&p)
			namespaceRegistry := namespace.NewMockRegistry(ctrl)

			namespaceConfig := &persistence.NamespaceConfig{
				Retention:             &p.Retention,
				HistoryArchivalUri:    "test:///history/archival/",
				VisibilityArchivalUri: "test:///visibility/archival",
			}
			if p.HistoryArchivalEnabledInNamespace {
				namespaceConfig.HistoryArchivalState = enums.ARCHIVAL_STATE_ENABLED
			} else {
				namespaceConfig.HistoryArchivalState = enums.ARCHIVAL_STATE_DISABLED
			}
			if p.VisibilityArchivalEnabledInNamespace {
				namespaceConfig.VisibilityArchivalState = enums.ARCHIVAL_STATE_ENABLED
			} else {
				namespaceConfig.VisibilityArchivalState = enums.ARCHIVAL_STATE_DISABLED
			}
			namespaceEntry := namespace.NewGlobalNamespaceForTest(
				&persistence.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
				namespaceConfig,
				&persistence.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters: []string{
						cluster.TestCurrentClusterName,
						cluster.TestAlternativeClusterName,
					},
				},
				tests.Version,
			)
			namespaceRegistry.EXPECT().GetNamespaceID(gomock.Any()).Return(namespaceEntry.ID(), nil).AnyTimes()
			namespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()

			mutableState := NewMockMutableState(ctrl)
			mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()
			mutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
			mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
				NamespaceId: namespaceEntry.ID().String(),
			}).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				namespaceEntry.ID().String(), tests.WorkflowID, tests.RunID,
			)).AnyTimes()
			mutableState.EXPECT().GetCurrentBranchToken().Return(nil, nil).AnyTimes()
			retentionTimerDelay := time.Second
			cfg := &configs.Config{
				RetentionTimerJitterDuration: func() time.Duration {
					return retentionTimerDelay
				},
				ArchivalProcessorArchiveDelay: func() time.Duration {
					return p.ArchivalProcessorArchiveDelay
				},
			}
			closeTime := time.Unix(0, 0)
			var allTasks []tasks.Task
			mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
				allTasks = append(allTasks, ts...)
			}).AnyTimes()

			archivalMetadata := archiver.NewMockArchivalMetadata(ctrl)
			archivalMetadata.EXPECT().GetHistoryConfig().DoAndReturn(func() archiver.ArchivalConfig {
				cfg := archiver.NewMockArchivalConfig(ctrl)
				cfg.EXPECT().ClusterConfiguredForArchival().Return(p.HistoryArchivalEnabledInCluster).AnyTimes()
				return cfg
			}).AnyTimes()
			archivalMetadata.EXPECT().GetVisibilityConfig().DoAndReturn(func() archiver.ArchivalConfig {
				cfg := archiver.NewMockArchivalConfig(ctrl)
				cfg.EXPECT().ClusterConfiguredForArchival().Return(p.VisibilityArchivalEnabledForCluster).AnyTimes()
				return cfg
			}).AnyTimes()

			taskGenerator := NewTaskGenerator(namespaceRegistry, mutableState, cfg, archivalMetadata)
			err := taskGenerator.GenerateWorkflowCloseTasks(&historypb.HistoryEvent{
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
				},
				EventTime: timestamp.TimePtr(p.CloseEventTime),
			}, p.DeleteAfterClose)
			require.NoError(t, err)

			var (
				closeExecutionTask           *tasks.CloseExecutionTask
				deleteHistoryEventTask       *tasks.DeleteHistoryEventTask
				closeExecutionVisibilityTask *tasks.CloseExecutionVisibilityTask
				archiveExecutionTask         *tasks.ArchiveExecutionTask
			)
			for _, task := range allTasks {
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
			assert.Equal(t, p.DeleteAfterClose, closeExecutionTask.DeleteAfterClose)
			assert.Equal(
				t,
				p.ExpectArchiveExecutionTask,
				closeExecutionTask.CanSkipVisibilityArchival,
			)

			if p.ExpectCloseExecutionVisibilityTask {
				assert.NotNil(t, closeExecutionVisibilityTask)
			} else {
				assert.Nil(t, closeExecutionVisibilityTask)
			}
			if p.ExpectArchiveExecutionTask {
				require.NotNil(t, archiveExecutionTask)
				assert.Equal(t, archiveExecutionTask.NamespaceID, namespaceEntry.ID().String())
				assert.Equal(t, archiveExecutionTask.WorkflowID, tests.WorkflowID)
				assert.Equal(t, archiveExecutionTask.RunID, tests.RunID)
				assert.True(t, p.ExpectedArchiveExecutionTaskVisibilityTimestamp.Equal(archiveExecutionTask.VisibilityTimestamp) ||
					p.ExpectedArchiveExecutionTaskVisibilityTimestamp.After(archiveExecutionTask.VisibilityTimestamp))
			} else {
				assert.Nil(t, archiveExecutionTask)
			}
			if p.ExpectDeleteHistoryEventTask {
				require.NotNil(t, deleteHistoryEventTask)
				assert.Equal(t, deleteHistoryEventTask.NamespaceID, namespaceEntry.ID().String())
				assert.Equal(t, deleteHistoryEventTask.WorkflowID, tests.WorkflowID)
				assert.Equal(t, deleteHistoryEventTask.RunID, tests.RunID)
				assert.GreaterOrEqual(t, deleteHistoryEventTask.VisibilityTimestamp, closeTime.Add(p.Retention))
				assert.LessOrEqual(t, deleteHistoryEventTask.VisibilityTimestamp,
					closeTime.Add(p.Retention).Add(retentionTimerDelay*2))
			} else {
				assert.Nil(t, deleteHistoryEventTask)
			}
		})
	}
}

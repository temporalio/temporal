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

package workflow

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/plugins/callbacks"
	"go.temporal.io/server/plugins/nexusoperations"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	t.Parallel()

	for _, c := range []testConfig{
		{
			Name: "archival enabled",
			ConfigFn: func(p *testParams) {
				p.ExpectCloseExecutionVisibilityTask = true
				p.ExpectArchiveExecutionTask = true
			},
		},
		{
			Name: "delete after close skips archival",
			ConfigFn: func(p *testParams) {
				p.DeleteAfterClose = true
				p.ExpectCloseExecutionVisibilityTask = false
				p.ExpectArchiveExecutionTask = false
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
			t.Parallel()

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

			namespaceConfig := &persistencespb.NamespaceConfig{
				Retention:             durationpb.New(p.Retention),
				HistoryArchivalUri:    "test:///history/archival/",
				VisibilityArchivalUri: "test:///visibility/archival",
			}
			if p.HistoryArchivalEnabledInNamespace {
				namespaceConfig.HistoryArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
			} else {
				namespaceConfig.HistoryArchivalState = enumspb.ARCHIVAL_STATE_DISABLED
			}
			if p.VisibilityArchivalEnabledInNamespace {
				namespaceConfig.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
			} else {
				namespaceConfig.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_DISABLED
			}
			namespaceEntry := namespace.NewGlobalNamespaceForTest(
				&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
				namespaceConfig,
				&persistencespb.NamespaceReplicationConfig{
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
			execState := &persistencespb.WorkflowExecutionState{
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			}
			mutableState.EXPECT().GetExecutionState().Return(execState).AnyTimes()
			mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()
			mutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
			mutableState.EXPECT().GetExecutionInfo().DoAndReturn(func() *persistencespb.WorkflowExecutionInfo {
				return &persistencespb.WorkflowExecutionInfo{
					NamespaceId: namespaceEntry.ID().String(),
				}
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
			err := taskGenerator.GenerateWorkflowCloseTasks(p.CloseEventTime, p.DeleteAfterClose)
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

func TestTaskGenerator_GenerateDirtySubStateMachineTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	subStateMachinesByType := map[int32]*persistencespb.StateMachineMap{}
	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterTaskSerializer(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	require.NoError(t, nexusoperations.RegisterTaskSerializer(reg))
	node, err := hsm.NewRoot(reg, StateMachineType.ID, nil, subStateMachinesByType)
	require.NoError(t, err)
	coll := callbacks.MachineCollection(node)

	callbackToSchedule := callbacks.NewCallback(timestamppb.Now(), callbacks.NewWorkflowClosedTrigger(), &common.Callback{
		Variant: &common.Callback_Nexus_{
			Nexus: &common.Callback_Nexus{
				Url: "http://localhost?foo=bar",
			},
		},
	})
	_, err = coll.Add("sched", callbackToSchedule)
	require.NoError(t, err)
	err = coll.Transition("sched", func(cb callbacks.Callback) (hsm.TransitionOutput, error) {
		return callbacks.TransitionScheduled.Apply(cb, callbacks.EventScheduled{})
	})
	require.NoError(t, err)

	callbackToBackoff := callbacks.NewCallback(timestamppb.Now(), callbacks.NewWorkflowClosedTrigger(), &common.Callback{
		Variant: &common.Callback_Nexus_{
			Nexus: &common.Callback_Nexus{
				Url: "http://localhost?foo=bar",
			},
		},
	})
	callbackToBackoff.PublicInfo.State = enumspb.CALLBACK_STATE_SCHEDULED
	_, err = coll.Add("backoff", callbackToBackoff)
	require.NoError(t, err)
	err = coll.Transition("backoff", func(cb callbacks.Callback) (hsm.TransitionOutput, error) {
		return callbacks.TransitionAttemptFailed.Apply(cb, callbacks.EventAttemptFailed{Time: time.Now(), Err: fmt.Errorf("test")}) // nolint:goerr113
	})
	require.NoError(t, err)

	mutableState := NewMockMutableState(ctrl)
	mutableState.EXPECT().HSM().DoAndReturn(func() *hsm.Node { return node }).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{
				NamespaceFailoverVersion: 3,
				MaxTransitionCount:       2,
			},
		},
	}).AnyTimes()
	mutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()

	cfg := &configs.Config{}
	archivalMetadata := archiver.NewMockArchivalMetadata(ctrl)

	var genTasks []tasks.Task
	mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
		genTasks = append(genTasks, ts...)
	}).AnyTimes()

	taskGenerator := NewTaskGenerator(namespaceRegistry, mutableState, cfg, archivalMetadata)
	err = taskGenerator.GenerateDirtySubStateMachineTasks(reg)
	require.NoError(t, err)

	require.Equal(t, 2, len(genTasks))
	invocationTask, ok := genTasks[0].(*tasks.StateMachineOutboundTask)
	var backoffTask *tasks.StateMachineTimerTask
	if ok {
		backoffTask = genTasks[1].(*tasks.StateMachineTimerTask)
	} else {
		invocationTask = genTasks[1].(*tasks.StateMachineOutboundTask)
		backoffTask = genTasks[0].(*tasks.StateMachineTimerTask)
	}
	require.Equal(t, tests.WorkflowKey, invocationTask.WorkflowKey)
	require.Equal(t, "http://localhost", invocationTask.Destination)
	require.Equal(t, &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: callbacks.StateMachineType.ID,
					Id:   "sched",
				},
			},
			MutableStateNamespaceFailoverVersion: 3,
			MutableStateTransitionCount:          2,
			MachineTransitionCount:               1,
		},
		Type: callbacks.TaskTypeInvocation.ID,
		Data: nil,
	}, invocationTask.Info)

	require.Equal(t, tests.WorkflowKey, backoffTask.WorkflowKey)
	require.Equal(t, &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: callbacks.StateMachineType.ID,
					Id:   "backoff",
				},
			},
			MutableStateNamespaceFailoverVersion: 3,
			MutableStateTransitionCount:          2,
			MachineTransitionCount:               1,
		},
		Type: callbacks.TaskTypeBackoff.ID,
		Data: nil,
	}, backoffTask.Info)

	// Reset and test a concurrent task (nexusoperations.TimeoutTask)
	node.ClearTransactionState()
	genTasks = nil
	opNode, err := nexusoperations.AddChild(node, "some-service", "some-op", timestamppb.Now(), durationpb.New(time.Hour))
	require.NoError(t, err)
	err = taskGenerator.GenerateDirtySubStateMachineTasks(reg)
	require.NoError(t, err)

	require.Equal(t, 2, len(genTasks))
	timeoutTask, ok := genTasks[0].(*tasks.StateMachineTimerTask)
	if !ok {
		timeoutTask = genTasks[1].(*tasks.StateMachineTimerTask)
	}

	require.Equal(t, tests.WorkflowKey, timeoutTask.WorkflowKey)
	require.Equal(t, &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: opNode.Key.Type,
					Id:   opNode.Key.ID,
				},
			},
			MutableStateNamespaceFailoverVersion: 3,
			MutableStateTransitionCount:          2,
			MachineTransitionCount:               0, // concurrent tasks don't store the machine transition count.
		},
		Type: nexusoperations.TaskTypeTimeout.ID,
		Data: nil,
	}, timeoutTask.Info)
}

package workflow

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
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

			mutableState := historyi.NewMockMutableState(ctrl)
			execState := &persistencespb.WorkflowExecutionState{
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			}
			mutableState.EXPECT().GetExecutionState().Return(execState).AnyTimes()
			mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()
			mutableState.EXPECT().GetCloseVersion().Return(int64(0), nil).AnyTimes()
			mutableState.EXPECT().GetExecutionInfo().DoAndReturn(func() *persistencespb.WorkflowExecutionInfo {
				return &persistencespb.WorkflowExecutionInfo{
					NamespaceId: namespaceEntry.ID().String(),
				}
			}).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				namespaceEntry.ID().String(), tests.WorkflowID, tests.RunID,
			)).AnyTimes()
			mutableState.EXPECT().GetCurrentBranchToken().Return(nil, nil).AnyTimes()
			mutableState.EXPECT().ChasmTree().Return(NoopChasmTree).AnyTimes()
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

			taskGenerator := NewTaskGenerator(namespaceRegistry, mutableState, cfg, archivalMetadata, log.NewTestLogger())
			err := taskGenerator.GenerateWorkflowCloseTasks(p.CloseEventTime, p.DeleteAfterClose, false)
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

	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetCurrentVersion().Return(int64(3)).AnyTimes()
	mutableState.EXPECT().NextTransitionCount().Return(int64(3)).AnyTimes()

	subStateMachinesByType := map[string]*persistencespb.StateMachineMap{}
	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterTaskSerializers(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	require.NoError(t, nexusoperations.RegisterTaskSerializers(reg))
	node, err := hsm.NewRoot(reg, StateMachineType, nil, subStateMachinesByType, mutableState)
	require.NoError(t, err)
	coll := callbacks.MachineCollection(node)

	callbackToSchedule := callbacks.NewCallback(
		"request-id-1",
		timestamppb.Now(),
		callbacks.NewWorkflowClosedTrigger(), &persistencespb.Callback{
			Variant: &persistencespb.Callback_Nexus_{
				Nexus: &persistencespb.Callback_Nexus{
					Url: "http://localhost?foo=bar",
				},
			},
		},
	)
	_, err = coll.Add("sched", callbackToSchedule)
	require.NoError(t, err)
	err = coll.Transition("sched", func(cb callbacks.Callback) (hsm.TransitionOutput, error) {
		return callbacks.TransitionScheduled.Apply(cb, callbacks.EventScheduled{})
	})
	require.NoError(t, err)

	callbackToBackoff := callbacks.NewCallback(
		"request-id-2",
		timestamppb.Now(),
		callbacks.NewWorkflowClosedTrigger(), &persistencespb.Callback{
			Variant: &persistencespb.Callback_Nexus_{
				Nexus: &persistencespb.Callback_Nexus{
					Url: "http://localhost?foo=bar",
				},
			},
		},
	)
	callbackToBackoff.CallbackInfo.State = enumsspb.CALLBACK_STATE_SCHEDULED
	_, err = coll.Add("backoff", callbackToBackoff)
	require.NoError(t, err)
	err = coll.Transition("backoff", func(cb callbacks.Callback) (hsm.TransitionOutput, error) {
		return callbacks.TransitionAttemptFailed.Apply(cb, callbacks.EventAttemptFailed{
			Time:        time.Now(),
			Err:         fmt.Errorf("test"),
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		})
	})
	require.NoError(t, err)

	mutableState.EXPECT().HSM().DoAndReturn(func() *hsm.Node { return node }).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 3, TransitionCount: 3},
		},
	}).AnyTimes()
	mutableState.EXPECT().CurrentVersionedTransition().Return(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 3, TransitionCount: 3,
	}).AnyTimes()
	mutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()

	cfg := &configs.Config{}
	archivalMetadata := archiver.NewMockArchivalMetadata(ctrl)

	var genTasks []tasks.Task
	mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
		genTasks = append(genTasks, ts...)
	}).AnyTimes()

	taskGenerator := NewTaskGenerator(namespaceRegistry, mutableState, cfg, archivalMetadata, log.NewTestLogger())
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
	protorequire.ProtoEqual(t, &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: callbacks.StateMachineType,
					Id:   "sched",
				},
			},
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineTransitionCount: 1,
		},
		Type: callbacks.TaskTypeInvocation,
		Data: nil,
	}, invocationTask.Info)

	require.Equal(t, tests.WorkflowKey, backoffTask.WorkflowKey)
	require.Equal(t, int64(3), backoffTask.Version)

	timers := mutableState.GetExecutionInfo().StateMachineTimers
	require.Equal(t, 1, len(timers))
	protorequire.ProtoEqual(t, &persistencespb.StateMachineTimerGroup{
		Deadline:  callbackToBackoff.NextAttemptScheduleTime,
		Scheduled: true,
		Infos: []*persistencespb.StateMachineTaskInfo{
			{
				Ref: &persistencespb.StateMachineRef{
					Path: []*persistencespb.StateMachineKey{
						{
							Type: callbacks.StateMachineType,
							Id:   "backoff",
						},
					},
					MutableStateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 3,
						TransitionCount:          3,
					},
					MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 3,
						TransitionCount:          3,
					},
					MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 3,
						TransitionCount:          3,
					},
					MachineTransitionCount: 1,
				},
				Type: callbacks.TaskTypeBackoff,
				Data: nil,
			},
		},
	}, timers[0])

	// Reset and test another timer task (nexusoperations.TimeoutTask)
	node.ClearTransactionState()
	genTasks = nil
	opNode, err := nexusoperations.AddChild(node, "ID", &historypb.HistoryEvent{
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Endpoint:               "endpoint",
				EndpointId:             "endpoint-id",
				Service:                "some-service",
				Operation:              "some-op",
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			},
		},
	}, []byte("token"))
	require.NoError(t, err)
	err = taskGenerator.GenerateDirtySubStateMachineTasks(reg)
	require.NoError(t, err)

	// No new timer tasks are generated they are collapsed.
	// Only an outbound task is expected here.
	require.Equal(t, 1, len(genTasks))
	_, ok = genTasks[0].(*tasks.StateMachineOutboundTask)
	require.True(t, ok)

	timers = mutableState.GetExecutionInfo().StateMachineTimers
	require.Equal(t, 2, len(timers))

	protorequire.ProtoEqual(t, &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: opNode.Key.Type,
					Id:   opNode.Key.ID,
				},
			},
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 3,
				TransitionCount:          3,
			},
			MachineTransitionCount: 1,
		},
		Type: nexusoperations.TaskTypeTimeout,
		Data: nil,
	}, timers[1].Infos[0])
}

func TestTaskGenerator_GenerateWorkflowStartTasks(t *testing.T) {
	testCases := []struct {
		name string

		executionTimerEnabled bool

		isFirstRun                   bool
		existingExecutionTimerStatus int32
		executionExpirationTime      time.Time
		runExpirationTime            time.Time

		expectedExecutionTimerStatus int32
		expectedTaskTypes            []enumsspb.TaskType
	}{
		{
			name:                         "execution timer disabled, no run expiration",
			executionTimerEnabled:        false,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusCreated,
			executionExpirationTime:      time.Time{},
			runExpirationTime:            time.Time{},
			expectedExecutionTimerStatus: TimerTaskStatusNone, // reset flag when execution timer is disabled
			expectedTaskTypes:            []enumsspb.TaskType{},
		},
		{
			name:                         "execution timer disabled, has run expiration",
			executionTimerEnabled:        false,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusCreated,
			executionExpirationTime:      time.Time{},
			runExpirationTime:            time.Now().Add(time.Minute),
			expectedExecutionTimerStatus: TimerTaskStatusNone,
			expectedTaskTypes:            []enumsspb.TaskType{enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT},
		},
		{
			name:                         "execution timer enabled and carried over, run expiration capped",
			executionTimerEnabled:        true,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusCreated,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Minute), // run expiration capped to execution expiration
			expectedExecutionTimerStatus: TimerTaskStatusCreated,
			expectedTaskTypes:            []enumsspb.TaskType{},
		},
		{
			name:                         "execution timer enabled and carried over, run expiration not capped",
			executionTimerEnabled:        true,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusCreated,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Second),
			expectedExecutionTimerStatus: TimerTaskStatusCreated,
			expectedTaskTypes:            []enumsspb.TaskType{enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT},
		},
		{
			name:                         "execution timer enabled but not carried over, run expiration capped",
			executionTimerEnabled:        true,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusNone,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Minute),
			expectedExecutionTimerStatus: TimerTaskStatusCreated,
			expectedTaskTypes:            []enumsspb.TaskType{enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT},
		},
		{
			name:                         "execution timer enabled but not carried over, run expiration not capped",
			executionTimerEnabled:        true,
			isFirstRun:                   false,
			existingExecutionTimerStatus: TimerTaskStatusNone,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Second),
			expectedExecutionTimerStatus: TimerTaskStatusCreated,
			expectedTaskTypes: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT,
				enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
			},
		},
		{
			name:                         "execution timer enabled, first run, run expiration capped",
			executionTimerEnabled:        true,
			isFirstRun:                   true,
			existingExecutionTimerStatus: TimerTaskStatusNone,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Minute),
			expectedExecutionTimerStatus: TimerTaskStatusNone,
			expectedTaskTypes: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
			},
		},
		{
			name:                         "execution timer enabled, first run, run expiration not capped",
			executionTimerEnabled:        true,
			isFirstRun:                   true,
			existingExecutionTimerStatus: TimerTaskStatusNone,
			executionExpirationTime:      time.Now().Add(time.Minute),
			runExpirationTime:            time.Now().Add(time.Second),
			expectedExecutionTimerStatus: TimerTaskStatusNone,
			expectedTaskTypes: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
			},
		},
		{
			name:                         "execution timer enabled, no execution expiration",
			executionTimerEnabled:        true,
			isFirstRun:                   true,
			existingExecutionTimerStatus: TimerTaskStatusNone,
			executionExpirationTime:      time.Time{},
			runExpirationTime:            time.Now().Add(time.Second),
			expectedExecutionTimerStatus: TimerTaskStatusNone,
			expectedTaskTypes: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)

			config := tests.NewDynamicConfig()
			if !tc.executionTimerEnabled {
				config.EnableWorkflowExecutionTimeoutTimer = dynamicconfig.GetBoolPropertyFn(false)
			}

			mockShard := shard.NewTestContext(
				controller,
				&persistencespb.ShardInfo{
					ShardId: 1,
					RangeId: 1,
				},
				config,
			)

			mockMutableState := historyi.NewMockMutableState(controller)
			mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

			firstRunID := uuid.NewString()
			currentRunID := firstRunID
			if !tc.isFirstRun {
				currentRunID = uuid.NewString()
			}

			workflowKey := tests.WorkflowKey
			mockMutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				NamespaceId:                      workflowKey.NamespaceID,
				WorkflowId:                       workflowKey.WorkflowID,
				WorkflowExecutionTimerTaskStatus: tc.existingExecutionTimerStatus,
				WorkflowExecutionExpirationTime:  timestamppb.New(tc.executionExpirationTime),
				WorkflowRunExpirationTime:        timestamppb.New(tc.runExpirationTime),
				FirstExecutionRunId:              firstRunID,
			}).AnyTimes()
			mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
				RunId: currentRunID,
			}).AnyTimes()

			var generatedTaskTypes []enumsspb.TaskType
			mockMutableState.EXPECT().AddTasks(gomock.Any()).Do(func(newTasks ...tasks.Task) {
				for _, newTask := range newTasks {
					generatedTaskTypes = append(generatedTaskTypes, newTask.GetType())
				}
			}).AnyTimes()

			taskGenerator := NewTaskGenerator(
				mockShard.GetNamespaceRegistry(),
				mockMutableState,
				mockShard.GetConfig(),
				mockShard.GetArchivalMetadata(),
				log.NewTestLogger(),
			)

			actualExecutionTimerTaskStatus, err := taskGenerator.GenerateWorkflowStartTasks(&historypb.HistoryEvent{
				EventId:   1,
				EventTime: timestamppb.Now(),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Version:   1,
				TaskId:    123,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
						WorkflowExecutionExpirationTime: timestamppb.New(tc.executionExpirationTime),
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, tc.expectedExecutionTimerStatus, actualExecutionTimerTaskStatus)
			require.ElementsMatch(t, tc.expectedTaskTypes, generatedTaskTypes)
		})
	}
}

func TestTaskGeneratorImpl_GenerateMigrationTasks(t *testing.T) {
	testCases := []struct {
		name                        string
		workflowState               enumsspb.WorkflowExecutionState
		transitionHistoryEnabled    bool
		pendingActivityInfo         map[int64]*persistencespb.ActivityInfo
		expectedTaskTypes           []enumsspb.TaskType
		expectedTaskEquivalentTypes []enumsspb.TaskType
	}{
		{
			name:                     "transition history disabled, execution completed",
			transitionHistoryEnabled: false,
			workflowState:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			pendingActivityInfo:      map[int64]*persistencespb.ActivityInfo{},
			expectedTaskTypes:        []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE},
		},
		{
			name:                        "transition history enabled, execution completed",
			transitionHistoryEnabled:    true,
			workflowState:               enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			pendingActivityInfo:         map[int64]*persistencespb.ActivityInfo{},
			expectedTaskTypes:           []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION},
			expectedTaskEquivalentTypes: []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE},
		},
		{
			name:                     "transition history enabled, execution running",
			transitionHistoryEnabled: true,
			workflowState:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			pendingActivityInfo: map[int64]*persistencespb.ActivityInfo{
				1: {
					Version:          1,
					ScheduledEventId: 1,
				},
				2: {
					Version:          1,
					ScheduledEventId: 2,
				},
			},
			expectedTaskTypes:           []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION},
			expectedTaskEquivalentTypes: []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_HISTORY, enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM},
		},
		{
			name:                     "transition history disabled, execution running",
			transitionHistoryEnabled: false,
			workflowState:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			pendingActivityInfo: map[int64]*persistencespb.ActivityInfo{
				1: {
					Version:          1,
					ScheduledEventId: 1,
				},
				2: {
					Version:          1,
					ScheduledEventId: 2,
				},
			},
			expectedTaskTypes: []enumsspb.TaskType{enumsspb.TASK_TYPE_REPLICATION_HISTORY, enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			mockMutableState := historyi.NewMockMutableState(controller)
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 10,
									Version: 1,
								},
							},
						},
					},
				},
				TransitionHistory: []*persistencespb.VersionedTransition{
					{
						NamespaceFailoverVersion: 1,
						TransitionCount:          5,
					},
				},
			}
			mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mockMutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
			mockMutableState.EXPECT().GetPendingActivityInfos().Return(tc.pendingActivityInfo).AnyTimes()
			mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
				State: tc.workflowState,
			}).AnyTimes()
			mockMutableState.EXPECT().IsTransitionHistoryEnabled().Return(tc.transitionHistoryEnabled).AnyTimes()
			mockMutableState.EXPECT().ChasmTree().Return(NoopChasmTree).AnyTimes()
			mockShard := shard.NewTestContext(
				controller,
				&persistencespb.ShardInfo{
					ShardId: 1,
					RangeId: 1,
				},
				tests.NewDynamicConfig(),
			)
			taskGenerator := NewTaskGenerator(
				mockShard.GetNamespaceRegistry(),
				mockMutableState,
				mockShard.GetConfig(),
				mockShard.GetArchivalMetadata(),
				log.NewTestLogger(),
			)
			resultTasks, _, err := taskGenerator.GenerateMigrationTasks(nil)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedTaskTypes), len(resultTasks))
			if tc.transitionHistoryEnabled {
				require.Equal(t, 1, len(resultTasks))
				require.Equal(t, tc.expectedTaskTypes[0].String(), resultTasks[0].GetType().String())
				syncVersionTask, ok := resultTasks[0].(*tasks.SyncVersionedTransitionTask)
				require.True(t, ok)
				require.Equal(t, chasm.WorkflowArchetypeID, syncVersionTask.GetArchetypeID())
				taskEquivalent := syncVersionTask.TaskEquivalents
				require.Equal(t, len(tc.expectedTaskEquivalentTypes), len(taskEquivalent))
				for i, equivalent := range taskEquivalent {
					require.Equal(t, tc.expectedTaskEquivalentTypes[i], equivalent.GetType())
				}
			} else {
				for i, task := range resultTasks {
					require.Equal(t, tc.expectedTaskTypes[i], task.GetType())
				}
			}
		})
	}
}

func TestTaskGeneratorImpl_GenerateDirtySubStateMachineTasks_TrimsTimersForDeletedNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ms := historyi.NewMockMutableState(ctrl)
	var genTasks []tasks.Task
	ms.EXPECT().AddTasks(gomock.Any()).DoAndReturn(func(tasks ...tasks.Task) {
		genTasks = append(genTasks, tasks...)
	}).AnyTimes()

	ms.EXPECT().IsTransitionHistoryEnabled().Return(true).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(3)).AnyTimes()

	currentTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 3,
		TransitionCount:          3,
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{currentTransition},
		StateMachineTimers: []*persistencespb.StateMachineTimerGroup{
			{
				Deadline: timestamppb.New(time.Now().Add(time.Hour)),
				Infos: []*persistencespb.StateMachineTaskInfo{
					{
						Ref: &persistencespb.StateMachineRef{
							Path: []*persistencespb.StateMachineKey{
								{Type: callbacks.StateMachineType, Id: "test-callback"},
							},
						},
						Type: callbacks.TaskTypeBackoff,
					},
				},
			},
		},
	}
	ms.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()

	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))

	cb := callbacks.NewCallback(
		"request-id",
		timestamppb.Now(),
		callbacks.NewWorkflowClosedTrigger(),
		&persistencespb.Callback{},
	)
	root, err := hsm.NewRoot(reg, StateMachineType, ms, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	child, err := callbacks.MachineCollection(root).Add("test-callback", cb)
	require.NoError(t, err)
	err = root.DeleteChild(child.Key)
	require.NoError(t, err)

	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
		tests.NamespaceID.String(),
		tests.WorkflowID,
		tests.RunID,
	)).AnyTimes()

	taskGenerator := NewTaskGenerator(
		namespace.NewMockRegistry(ctrl),
		ms,
		&configs.Config{},
		archiver.NewMockArchivalMetadata(ctrl),
		log.NewTestLogger(),
	)

	err = taskGenerator.GenerateDirtySubStateMachineTasks(reg)
	require.NoError(t, err)

	require.Empty(t, genTasks)
	require.Empty(t, ms.GetExecutionInfo().StateMachineTimers) // Timer should be trimmed
}

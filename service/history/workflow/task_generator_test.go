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
	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
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
		Type: nexusoperations.TaskTypeScheduleToCloseTimeout,
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

func TestTaskGeneratorImpl_GenerateDeleteHistoryEventTask_ChasmComponentRetention(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	closeTime := time.Unix(0, 0)
	retentionJitterDuration := time.Second

	testCases := []struct {
		name                   string
		archetypeID            chasm.ArchetypeID
		expectedMinRetention   time.Duration
		expectedMaxRetention   time.Duration
		setupNamespaceRegistry func(*namespace.MockRegistry)
	}{
		{
			name:                 "standalone activity uses namespace retention",
			archetypeID:          activity.ArchetypeID,
			expectedMinRetention: 90 * 24 * time.Hour,
			expectedMaxRetention: 90*24*time.Hour + retentionJitterDuration*2,
			setupNamespaceRegistry: func(nr *namespace.MockRegistry) {
				namespaceConfig := &persistencespb.NamespaceConfig{
					Retention: durationpb.New(90 * 24 * time.Hour),
				}
				namespaceEntry := namespace.NewGlobalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
					namespaceConfig,
					&persistencespb.NamespaceReplicationConfig{
						ActiveClusterName: cluster.TestCurrentClusterName,
						Clusters: []string{
							cluster.TestCurrentClusterName,
						},
					},
					tests.Version,
				)
				nr.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
			},
		},
		{
			name:                 "workflow uses namespace retention",
			archetypeID:          chasm.WorkflowArchetypeID,
			expectedMinRetention: 7 * 24 * time.Hour,
			expectedMaxRetention: 7*24*time.Hour + retentionJitterDuration*2,
			setupNamespaceRegistry: func(nr *namespace.MockRegistry) {
				namespaceConfig := &persistencespb.NamespaceConfig{
					Retention: durationpb.New(7 * 24 * time.Hour),
				}
				namespaceEntry := namespace.NewGlobalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
					namespaceConfig,
					&persistencespb.NamespaceReplicationConfig{
						ActiveClusterName: cluster.TestCurrentClusterName,
						Clusters: []string{
							cluster.TestCurrentClusterName,
						},
					},
					tests.Version,
				)
				nr.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
			},
		},
		{
			name:                 "scheduler uses namespace retention",
			archetypeID:          chasm.SchedulerArchetypeID,
			expectedMinRetention: 30 * 24 * time.Hour,
			expectedMaxRetention: 30*24*time.Hour + retentionJitterDuration*2,
			setupNamespaceRegistry: func(nr *namespace.MockRegistry) {
				namespaceConfig := &persistencespb.NamespaceConfig{
					Retention: durationpb.New(30 * 24 * time.Hour),
				}
				namespaceEntry := namespace.NewGlobalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
					namespaceConfig,
					&persistencespb.NamespaceReplicationConfig{
						ActiveClusterName: cluster.TestCurrentClusterName,
						Clusters: []string{
							cluster.TestCurrentClusterName,
						},
					},
					tests.Version,
				)
				nr.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			tc.setupNamespaceRegistry(namespaceRegistry)

			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetExecutionInfo().DoAndReturn(func() *persistencespb.WorkflowExecutionInfo {
				return &persistencespb.WorkflowExecutionInfo{
					NamespaceId: tests.NamespaceID.String(),
				}
			}).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				tests.NamespaceID.String(), tests.WorkflowID, tests.RunID,
			)).AnyTimes()
			mutableState.EXPECT().GetCloseVersion().Return(int64(0), nil).AnyTimes()
			mutableState.EXPECT().GetCurrentBranchToken().Return([]byte("branch-token"), nil).AnyTimes()

			// Create a mock ChasmTree that returns the specific archetype ID
			mockChasmTree := historyi.NewMockChasmTree(ctrl)
			mockChasmTree.EXPECT().ArchetypeID().Return(tc.archetypeID).AnyTimes()
			mutableState.EXPECT().ChasmTree().Return(mockChasmTree).AnyTimes()

			var allTasks []tasks.Task
			mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
				allTasks = append(allTasks, ts...)
			}).AnyTimes()

			cfg := &configs.Config{
				RetentionTimerJitterDuration: func() time.Duration {
					return retentionJitterDuration
				},
			}

			taskGenerator := NewTaskGenerator(
				namespaceRegistry,
				mutableState,
				cfg,
				nil, // archivalMetadata not needed for this test
				testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
			)

			err := taskGenerator.GenerateDeleteHistoryEventTask(closeTime)
			require.NoError(t, err)

			// Find the DeleteHistoryEventTask
			var deleteHistoryEventTask *tasks.DeleteHistoryEventTask
			for _, task := range allTasks {
				if t, ok := task.(*tasks.DeleteHistoryEventTask); ok {
					deleteHistoryEventTask = t
					break
				}
			}

			require.NotNil(t, deleteHistoryEventTask, "DeleteHistoryEventTask should be created")
			assert.Equal(t, tests.NamespaceID.String(), deleteHistoryEventTask.NamespaceID)
			assert.Equal(t, tests.WorkflowID, deleteHistoryEventTask.WorkflowID)
			assert.Equal(t, tests.RunID, deleteHistoryEventTask.RunID)
			assert.Equal(t, tc.archetypeID, deleteHistoryEventTask.ArchetypeID)

			// Verify the retention time is within expected range
			expectedDeleteTime := closeTime.Add(tc.expectedMinRetention)
			assert.GreaterOrEqual(t, deleteHistoryEventTask.VisibilityTimestamp, expectedDeleteTime,
				"Delete time should be at least closeTime + retention")
			assert.LessOrEqual(t, deleteHistoryEventTask.VisibilityTimestamp, closeTime.Add(tc.expectedMaxRetention),
				"Delete time should not exceed closeTime + retention + jitter")
		})
	}
}

func TestGenerateWorkerCommandsTasks(t *testing.T) {
	t.Parallel()

	token1 := []byte("token1")
	token2 := []byte("token2")
	token3 := []byte("token3")

	makeCommands := func(tokens ...[]byte) []*workerpb.WorkerCommand {
		commands := make([]*workerpb.WorkerCommand, 0, len(tokens))
		for _, token := range tokens {
			commands = append(commands, &workerpb.WorkerCommand{
				Type: &workerpb.WorkerCommand_CancelActivity{
					CancelActivity: &workerpb.CancelActivityCommand{
						TaskToken: token,
					},
				},
			})
		}
		return commands
	}

	testCases := []struct {
		name           string
		featureEnabled bool
		commands       []*workerpb.WorkerCommand
		controlQueue   string
		expectTask     bool
	}{
		{
			name:           "creates task when enabled with valid inputs",
			featureEnabled: true,
			commands:       makeCommands(token1, token2, token3),
			controlQueue:   "test-control-queue",
			expectTask:     true,
		},
		{
			name:           "no task when feature disabled",
			featureEnabled: false,
			commands:       makeCommands(token1, token2, token3),
			controlQueue:   "test-control-queue",
			expectTask:     false,
		},
		{
			name:           "no task when commands empty",
			featureEnabled: true,
			commands:       []*workerpb.WorkerCommand{},
			controlQueue:   "test-control-queue",
			expectTask:     false,
		},
		{
			name:           "no task when controlQueue empty",
			featureEnabled: true,
			commands:       makeCommands(token1, token2, token3),
			controlQueue:   "",
			expectTask:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				tests.NamespaceID.String(), tests.WorkflowID, tests.RunID,
			)).AnyTimes()

			var capturedTasks []tasks.Task
			if tc.expectTask {
				mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
					capturedTasks = append(capturedTasks, ts...)
				}).Times(1)
			}

			cfg := &configs.Config{
				EnableCancelActivityWorkerCommand: func() bool { return tc.featureEnabled },
			}

			taskGenerator := NewTaskGenerator(nil, mutableState, cfg, nil, log.NewTestLogger())
			err := taskGenerator.GenerateWorkerCommandsTasks(tc.commands, tc.controlQueue)
			require.NoError(t, err)

			if tc.expectTask {
				require.Len(t, capturedTasks, 1)
				commandTask, ok := capturedTasks[0].(*tasks.WorkerCommandsTask)
				require.True(t, ok)
				require.Equal(t, tc.commands, commandTask.Commands)
				require.Equal(t, tc.controlQueue, commandTask.Destination)
				require.Equal(t, tests.NamespaceID.String(), commandTask.NamespaceID)
			}
		})
	}
}

func TestTaskGeneratorImpl_RegenerateTimerTasksForTimeSkipping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	skippedDuration := time.Hour

	// Two pending timers: expiry at now+1h and now+2h. Every pending user timer
	// must be regenerated (not just the earliest), because the user may turn off time skipping after the first timer is started.
	// And the second timer should still be fired in the correct time after time skipping is turned off.
	timer1ExpiryTime := now.Add(1 * time.Hour)
	timer2ExpiryTime := now.Add(2 * time.Hour)

	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
			AccumulatedSkippedDuration: durationpb.New(skippedDuration),
		},
	}).AnyTimes()
	mutableState.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
		"timer-1": {
			StartedEventId: 1,
			ExpiryTime:     timestamppb.New(timer1ExpiryTime),
		},
		"timer-2": {
			StartedEventId: 2,
			ExpiryTime:     timestamppb.New(timer2ExpiryTime),
		},
	}).AnyTimes()
	mutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()

	var capturedTasks []tasks.Task
	mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
		capturedTasks = append(capturedTasks, ts...)
	}).AnyTimes()

	taskGenerator := NewTaskGenerator(nil, mutableState, &configs.Config{}, nil, log.NewTestLogger())
	require.NoError(t, taskGenerator.RegenerateTimerTasksForTimeSkipping())

	// Both pending user timers must be regenerated.
	require.Len(t, capturedTasks, 2)

	// Index captured tasks by EventID — emission order follows the sorted
	// sequence, but the assertions below don't need to depend on it.
	byEventID := make(map[int64]*tasks.UserTimerTask, len(capturedTasks))
	for _, task := range capturedTasks {
		ut, ok := task.(*tasks.UserTimerTask)
		require.True(t, ok, "expected *tasks.UserTimerTask, got %T", task)
		require.Equal(t, tests.WorkflowKey, ut.WorkflowKey)
		// TaskID must be zero: the generator leaves it unset; the shard assigns the real ID.
		require.Equal(t, int64(0), ut.TaskID, "TaskID must be zero (set by shard, not the generator)")
		byEventID[ut.EventID] = ut
	}

	// Task generator emits timer tasks with virtual-frame VisibilityTimestamp;
	// the virtual-to-wallclock subtraction happens inside MutableState.AddTasks,
	// which is a mock here and therefore records the pre-subtraction value.
	require.Contains(t, byEventID, int64(1))
	require.Equal(t, timer1ExpiryTime, byEventID[1].VisibilityTimestamp)
	require.Contains(t, byEventID, int64(2))
	require.Equal(t, timer2ExpiryTime, byEventID[2].VisibilityTimestamp)
}

func TestTaskGeneratorImpl_RegenerateTimerTasksForTimeSkipping_EdgeCases(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		execInfo    *persistencespb.WorkflowExecutionInfo
		setupTimers func(mutableState *historyi.MockMutableState)
	}{
		{
			name:     "nil TimeSkippingInfo returns immediately without touching timers",
			execInfo: &persistencespb.WorkflowExecutionInfo{TimeSkippingInfo: nil},
			// GetPendingTimerInfos and AddTasks must not be called — no expectations set.
		},
		{
			name: "zero AccumulatedSkippedDuration returns immediately without touching timers",
			execInfo: &persistencespb.WorkflowExecutionInfo{
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
					AccumulatedSkippedDuration: durationpb.New(0),
				},
			},
			// GetPendingTimerInfos and AddTasks must not be called — no expectations set.
		},
		{
			name: "no pending timers produces no tasks",
			execInfo: &persistencespb.WorkflowExecutionInfo{
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
					AccumulatedSkippedDuration: durationpb.New(time.Hour),
				},
			},
			setupTimers: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{})
				// AddTasks must not be called — no expectation set.
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetExecutionInfo().Return(tc.execInfo).AnyTimes()
			if tc.setupTimers != nil {
				tc.setupTimers(mutableState)
			}

			taskGenerator := NewTaskGenerator(nil, mutableState, &configs.Config{}, nil, log.NewTestLogger())
			require.NoError(t, taskGenerator.RegenerateTimerTasksForTimeSkipping())
		})
	}
}

// TestTaskGeneratorImpl_RegenerateTimerTasksForTimeSkipping_TimeoutTimers tests
// execution and run timeout timers are regenerated when the workflow skips time.
func TestTaskGeneratorImpl_RegenerateTimerTasksForTimeSkipping_ExecutionTimers(t *testing.T) {
	t.Parallel()

	const (
		namespaceID     = "ns-id"
		workflowID      = "wf-id"
		firstRunID      = "first-run-id"
		skippedDuration = time.Hour
		startVersion    = int64(7)
	)
	now := time.Now().UTC()
	userTimerExpiry := now.Add(1 * time.Hour)
	execExpiry := now.Add(24 * time.Hour)
	runExpiry := now.Add(3 * time.Hour)

	for _, tc := range []struct {
		name               string
		execExpirationTime *timestamppb.Timestamp
		runExpirationTime  *timestamppb.Timestamp
		wantExecTimeout    bool
		wantRunTimeout     bool
	}{
		{
			name:               "both execution and run expirations set",
			execExpirationTime: timestamppb.New(execExpiry),
			runExpirationTime:  timestamppb.New(runExpiry),
			wantExecTimeout:    true,
			wantRunTimeout:     true,
		},
		{
			name:               "only run expiration set",
			execExpirationTime: nil,
			runExpirationTime:  timestamppb.New(runExpiry),
			wantExecTimeout:    false,
			wantRunTimeout:     true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				NamespaceId:                     namespaceID,
				WorkflowId:                      workflowID,
				FirstExecutionRunId:             firstRunID,
				WorkflowExecutionExpirationTime: tc.execExpirationTime,
				WorkflowRunExpirationTime:       tc.runExpirationTime,
				TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
					AccumulatedSkippedDuration: durationpb.New(skippedDuration),
				},
			}).AnyTimes()
			mutableState.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
				"timer-1": {StartedEventId: 1, ExpiryTime: timestamppb.New(userTimerExpiry)},
			}).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
			// GetStartVersion is consulted when a WorkflowRunTimeoutTask is regenerated so the
			// task can pass CheckTaskVersion in multi-cluster deployments.
			if tc.wantRunTimeout {
				mutableState.EXPECT().GetStartVersion().Return(int64(startVersion), nil)
			}

			var captured []tasks.Task
			mutableState.EXPECT().AddTasks(gomock.Any()).Do(func(ts ...tasks.Task) {
				captured = append(captured, ts...)
			}).AnyTimes()

			taskGenerator := NewTaskGenerator(nil, mutableState, &configs.Config{}, nil, log.NewTestLogger())
			require.NoError(t, taskGenerator.RegenerateTimerTasksForTimeSkipping())

			// Expected task count: always user timer, plus any timeout tasks whose expiration is set.
			expectedCount := 1
			if tc.wantExecTimeout {
				expectedCount++
			}
			if tc.wantRunTimeout {
				expectedCount++
			}
			require.Len(t, captured, expectedCount)

			// User timer is always first (emission order in the implementation).
			userTask, ok := captured[0].(*tasks.UserTimerTask)
			require.True(t, ok, "first task should be *tasks.UserTimerTask, got %T", captured[0])
			require.Equal(t, int64(1), userTask.EventID)
			require.Equal(t, userTimerExpiry, userTask.VisibilityTimestamp)

			// Walk remaining tasks and verify each expected timeout appears exactly once with correct fields.
			var foundExec, foundRun bool
			for _, task := range captured[1:] {
				switch tt := task.(type) {
				case *tasks.WorkflowExecutionTimeoutTask:
					require.False(t, foundExec, "WorkflowExecutionTimeoutTask emitted more than once")
					foundExec = true
					require.Equal(t, namespaceID, tt.NamespaceID)
					require.Equal(t, workflowID, tt.WorkflowID)
					require.Equal(t, firstRunID, tt.FirstRunID)
					require.Equal(t, execExpiry, tt.VisibilityTimestamp)
					require.Equal(t, int64(0), tt.TaskID, "TaskID must be zero (set by shard)")
				case *tasks.WorkflowRunTimeoutTask:
					require.False(t, foundRun, "WorkflowRunTimeoutTask emitted more than once")
					foundRun = true
					require.Equal(t, tests.WorkflowKey, tt.WorkflowKey)
					require.Equal(t, runExpiry, tt.VisibilityTimestamp)
					require.Equal(t, int64(0), tt.TaskID, "TaskID must be zero (set by shard)")
					require.Equal(t, int64(startVersion), tt.Version, "Version must match start version so CheckTaskVersion passes")
				default:
					t.Fatalf("unexpected task type %T", task)
				}
			}
			require.Equal(t, tc.wantExecTimeout, foundExec)
			require.Equal(t, tc.wantRunTimeout, foundRun)
		})
	}
}

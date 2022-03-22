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

package persistencetests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/service/history/tasks"
)

// TimePrecision is needed to account for database timestamp precision.
// Cassandra only provides milliseconds timestamp precision, so we need to use tolerance when doing comparison
const TimePrecision = 2 * time.Millisecond

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GenerateTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		SQLDBPluginName   string
		DBName            string
		DBUsername        string
		DBPassword        string
		DBHost            string
		DBPort            int `yaml:"-"`
		ConnectAttributes map[string]string
		StoreType         string                 `yaml:"-"`
		SchemaDir         string                 `yaml:"-"`
		FaultInjection    *config.FaultInjection `yaml:"faultinjection"`
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr                  persistence.ShardManager
		AbstractDataStoreFactory  client.AbstractDataStoreFactory
		FaultInjection            *client.FaultInjectionDataStoreFactory
		Factory                   client.Factory
		ExecutionManager          persistence.ExecutionManager
		TaskMgr                   persistence.TaskManager
		ClusterMetadataManager    persistence.ClusterMetadataManager
		MetadataManager           persistence.MetadataManager
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		ShardInfo                 *persistencespb.ShardInfo
		TaskIDGenerator           TransferTaskIDGenerator
		ClusterMetadata           cluster.Metadata
		SearchAttributesManager   searchattribute.Manager
		ReadLevel                 int64
		ReplicationReadLevel      int64
		DefaultTestCluster        PersistenceTestCluster
		Logger                    log.Logger
	}

	// PersistenceTestCluster exposes management operations on a database
	PersistenceTestCluster interface {
		SetupTestDatabase()
		TearDownTestDatabase()
		Config() config.Persistence
	}

	// TestTransferTaskIDGenerator helper
	TestTransferTaskIDGenerator struct {
		seqNum int64
	}
)

const (
	defaultScheduleToStartTimeout = 111
)

// NewTestBaseWithCassandra returns a persistence test base backed by cassandra datastore
func NewTestBaseWithCassandra(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(3)
	}
	logger := log.NewTestLogger()
	testCluster := cassandra.NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, options.FaultInjection, logger)
	return NewTestBaseForCluster(testCluster, logger)
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(3)
	}
	logger := log.NewTestLogger()

	if options.DBPort == 0 {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBPort = environment.GetMySQLPort()
		case postgresql.PluginName:
			options.DBPort = environment.GetPostgreSQLPort()
		case sqlite.PluginName:
			options.DBPort = 0
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", options.SQLDBPluginName))
		}
	}
	if options.DBHost == "" {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBHost = environment.GetMySQLAddress()
		case postgresql.PluginName:
			options.DBHost = environment.GetPostgreSQLAddress()
		case sqlite.PluginName:
			options.DBHost = environment.Localhost
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", options.SQLDBPluginName))
		}
	}
	testCluster := sql.NewTestCluster(options.SQLDBPluginName, options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.ConnectAttributes, options.SchemaDir, options.FaultInjection, logger)
	return NewTestBaseForCluster(testCluster, logger)
}

// NewTestBase returns a persistence test base backed by either cassandra or sql
func NewTestBase(options *TestBaseOptions) TestBase {
	switch options.StoreType {
	case config.StoreTypeSQL:
		return NewTestBaseWithSQL(options)
	case config.StoreTypeNoSQL:
		return NewTestBaseWithCassandra(options)
	default:
		panic("invalid storeType " + options.StoreType)
	}
}

func NewTestBaseForCluster(testCluster PersistenceTestCluster, logger log.Logger) TestBase {
	return TestBase{
		DefaultTestCluster: testCluster,
		Logger:             logger,
	}
}

// Setup sets up the test base, must be called as part of SetupSuite
func (s *TestBase) Setup(clusterMetadataConfig *cluster.Config) {
	var err error
	shardID := int32(10)
	if clusterMetadataConfig == nil {
		clusterMetadataConfig = cluster.NewTestClusterMetadataConfig(false, false)
	}

	clusterName := clusterMetadataConfig.CurrentClusterName

	s.DefaultTestCluster.SetupTestDatabase()

	cfg := s.DefaultTestCluster.Config()
	scope := tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
	var metricsClient metrics.Client
	metricsClient, err = metrics.NewClient(&metrics.ClientConfig{}, scope, metrics.GetMetricsServiceIdx(common.HistoryServiceName, s.Logger))
	s.fatalOnError("metrics.NewClient", err)
	dataStoreFactory, faultInjection := client.DataStoreFactoryProvider(
		client.ClusterName(clusterName),
		resolver.NewNoopResolver(),
		&cfg,
		s.AbstractDataStoreFactory,
		s.Logger,
		metricsClient,
	)
	factory := client.NewFactory(dataStoreFactory, &cfg, nil, serialization.NewSerializer(), clusterName, metricsClient, s.Logger)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.ClusterMetadataManager, err = factory.NewClusterMetadataManager()
	s.fatalOnError("NewClusterMetadataManager", err)

	s.ClusterMetadata = cluster.NewMetadataFromConfig(clusterMetadataConfig, s.ClusterMetadataManager, dynamicconfig.NewNoopCollection(), s.Logger)
	s.SearchAttributesManager = searchattribute.NewManager(clock.NewRealTimeSource(), s.ClusterMetadataManager, dynamicconfig.GetBoolPropertyFn(true))

	s.MetadataManager, err = factory.NewMetadataManager()
	s.fatalOnError("NewMetadataManager", err)

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionManager, err = factory.NewExecutionManager()
	s.fatalOnError("NewExecutionManager", err)

	s.Factory = factory
	s.FaultInjection = faultInjection

	s.ReadLevel = 0
	s.ReplicationReadLevel = 0
	s.ShardInfo = &persistencespb.ShardInfo{
		ShardId:                 shardID,
		RangeId:                 0,
		TransferAckLevel:        0,
		ReplicationAckLevel:     0,
		TimerAckLevelTime:       timestamp.TimePtr(time.Unix(0, 0).UTC()),
		ClusterTimerAckLevel:    map[string]*time.Time{},
		ClusterTransferAckLevel: map[string]int64{clusterName: 0},
	}

	s.TaskIDGenerator = &TestTransferTaskIDGenerator{}
	_, err = s.ShardMgr.GetOrCreateShard(context.Background(), &persistence.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: s.ShardInfo,
	})
	s.fatalOnError("CreateShard", err)

	queue, err := factory.NewNamespaceReplicationQueue()
	s.fatalOnError("Create NamespaceReplicationQueue", err)
	s.NamespaceReplicationQueue = queue
}

func (s *TestBase) fatalOnError(msg string, err error) {
	if err != nil {
		s.Logger.Fatal(msg, tag.Error(err))
	}
}

// GetOrCreateShard is a utility method to get/create the shard using persistence layer
func (s *TestBase) GetOrCreateShard(ctx context.Context, shardID int32, owner string, rangeID int64) (*persistencespb.ShardInfo, error) {
	info := &persistencespb.ShardInfo{
		ShardId: shardID,
		Owner:   owner,
		RangeId: rangeID,
	}
	resp, err := s.ShardMgr.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: info,
	})
	if err != nil {
		return nil, err
	}
	return resp.ShardInfo, nil
}

// UpdateShard is a utility method to update the shard using persistence layer
func (s *TestBase) UpdateShard(ctx context.Context, updatedInfo *persistencespb.ShardInfo, previousRangeID int64) error {
	return s.ShardMgr.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo:       updatedInfo,
		PreviousRangeID: previousRangeID,
	})
}

// CreateWorkflowExecutionWithBranchToken test util function
func (s *TestBase) CreateWorkflowExecutionWithBranchToken(ctx context.Context, namespaceID string, workflowExecution commonpb.WorkflowExecution, taskQueue,
	wType string, wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleID int64, branchToken []byte, timerTasks []tasks.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(ctx, &persistence.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskQueue,
				WorkflowTypeName:           wType,
				WorkflowRunTimeout:         wTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: nextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: {
					&tasks.WorkflowTask{
						WorkflowKey: definition.NewWorkflowKey(
							namespaceID,
							workflowExecution.WorkflowId,
							workflowExecution.RunId,
						),
						TaskID:              s.GetNextSequenceNumber(),
						TaskQueue:           taskQueue,
						ScheduleID:          workflowTaskScheduleID,
						VisibilityTimestamp: time.Now().UTC(),
					},
				},
				tasks.CategoryTimer: timerTasks,
			},
			Checksum: testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(ctx context.Context, namespaceID string, workflowExecution commonpb.WorkflowExecution, taskQueue, wType string, wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID, lastProcessedEventID, workflowTaskScheduleID int64, timerTasks []tasks.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	return s.CreateWorkflowExecutionWithBranchToken(ctx, namespaceID, workflowExecution, taskQueue, wType, wTimeout, workflowTaskTimeout,
		nextEventID, lastProcessedEventID, workflowTaskScheduleID, nil, timerTasks)
}

// CreateWorkflowExecutionManyTasks is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionManyTasks(ctx context.Context, namespaceID string, workflowExecution commonpb.WorkflowExecution,
	taskQueue string, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64) (*persistence.CreateWorkflowExecutionResponse, error) {

	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)
	transferTasks := []tasks.Task{}
	for _, workflowTaskScheduleID := range workflowTaskScheduleIDs {
		transferTasks = append(transferTasks,
			&tasks.WorkflowTask{
				WorkflowKey: workflowKey,
				TaskID:      s.GetNextSequenceNumber(),
				TaskQueue:   taskQueue,
				ScheduleID:  workflowTaskScheduleID,
			})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks,
			&tasks.ActivityTask{
				WorkflowKey:       workflowKey,
				TaskID:            s.GetNextSequenceNumber(),
				TargetNamespaceID: namespaceID,
				TaskQueue:         taskQueue,
				ScheduleID:        activityScheduleID,
			})
	}

	response, err := s.ExecutionManager.CreateWorkflowExecution(ctx, &persistence.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:             namespaceID,
				WorkflowId:              workflowExecution.GetWorkflowId(),
				TaskQueue:               taskQueue,
				LastFirstEventId:        common.FirstEventID,
				LastWorkflowTaskStartId: lastProcessedEventID,
				WorkflowTaskScheduleId:  common.EmptyEventID,
				WorkflowTaskStartedId:   common.EmptyEventID,
				WorkflowTaskTimeout:     timestamp.DurationFromSeconds(1),
				ExecutionStats:          &persistencespb.ExecutionStats{},
				StartTime:               timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Checksum: testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// CreateChildWorkflowExecution is a utility method to create child workflow executions
func (s *TestBase) CreateChildWorkflowExecution(ctx context.Context, namespaceID string, workflowExecution commonpb.WorkflowExecution,
	parentNamespaceID string, parentExecution commonpb.WorkflowExecution, initiatedID int64, taskQueue, wType string,
	wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleID int64, timerTasks []tasks.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(ctx, &persistence.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				FirstExecutionRunId:        workflowExecution.GetRunId(),
				ParentNamespaceId:          parentNamespaceID,
				ParentWorkflowId:           parentExecution.GetWorkflowId(),
				ParentRunId:                parentExecution.GetRunId(),
				InitiatedId:                initiatedID,
				TaskQueue:                  taskQueue,
				WorkflowTypeName:           wType,
				WorkflowRunTimeout:         wTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: nextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: {
					&tasks.WorkflowTask{
						WorkflowKey: definition.NewWorkflowKey(
							namespaceID,
							workflowExecution.WorkflowId,
							workflowExecution.RunId,
						),
						TaskID:     s.GetNextSequenceNumber(),
						TaskQueue:  taskQueue,
						ScheduleID: workflowTaskScheduleID,
					},
				},
				tasks.CategoryTimer: timerTasks,
			},
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// GetWorkflowExecutionInfo is a utility method to retrieve execution info
func (s *TestBase) GetWorkflowMutableState(ctx context.Context, namespaceID string, workflowExecution commonpb.WorkflowExecution) (
	*persistencespb.WorkflowMutableState, error) {
	response, err := s.ExecutionManager.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.ShardInfo.GetShardId(),
		NamespaceID: namespaceID,
		WorkflowID:  workflowExecution.GetWorkflowId(),
		RunID:       workflowExecution.GetRunId(),
	})
	if err != nil {
		return nil, err
	}
	return response.State, nil
}

// GetCurrentWorkflowRunID returns the workflow run ID for the given params
func (s *TestBase) GetCurrentWorkflowRunID(ctx context.Context, namespaceID, workflowID string) (string, error) {
	response, err := s.ExecutionManager.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID:     s.ShardInfo.GetShardId(),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})

	if err != nil {
		return "", err
	}

	return response.RunID, nil
}

// ContinueAsNewExecution is a utility method to create workflow executions
func (s *TestBase) ContinueAsNewExecution(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo, updatedState *persistencespb.WorkflowExecutionState, updatedNextEventID int64, condition int64,
	newExecution commonpb.WorkflowExecution, nextEventID, workflowTaskScheduleID int64,
	prevResetPoints *workflowpb.ResetPoints) error {
	newworkflowTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			updatedInfo.NamespaceId,
			updatedInfo.WorkflowId,
			updatedState.RunId,
		),
		TaskID:     s.GetNextSequenceNumber(),
		TaskQueue:  updatedInfo.TaskQueue,
		ScheduleID: workflowTaskScheduleID,
	}

	req := &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    updatedNextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: {newworkflowTask},
			},
			Condition:           condition,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
		},
		NewWorkflowSnapshot: &persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                updatedInfo.NamespaceId,
				WorkflowId:                 newExecution.GetWorkflowId(),
				TaskQueue:                  updatedInfo.TaskQueue,
				WorkflowTypeName:           updatedInfo.WorkflowTypeName,
				WorkflowRunTimeout:         updatedInfo.WorkflowRunTimeout,
				DefaultWorkflowTaskTimeout: updatedInfo.DefaultWorkflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    common.EmptyEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				AutoResetPoints:            prevResetPoints,
				StartTime:                  timestamp.TimeNowPtrUtc(),
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           newExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           updatedState.State,
				Status:          updatedState.Status,
			},
			Tasks: map[tasks.Category][]tasks.Task{},
		},
		RangeID: s.ShardInfo.GetRangeId(),
	}
	req.UpdateWorkflowMutation.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	req.UpdateWorkflowMutation.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, req)
	return err
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, condition int64, timerTasks []tasks.Task,
	upsertActivityInfos []*persistencespb.ActivityInfo, deleteActivityInfos []int64,
	upsertTimerInfos []*persistencespb.TimerInfo, deleteTimerInfos []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, workflowTaskScheduleIDs, activityScheduleIDs,
		s.ShardInfo.GetRangeId(), condition, timerTasks, upsertActivityInfos, deleteActivityInfos,
		upsertTimerInfos, deleteTimerInfos, nil, nil, nil, nil,
		nil, nil, nil, nil)
}

// UpdateWorkflowExecutionAndFinish is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionAndFinish(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64, condition int64) error {
	var transferTasks []tasks.Task
	transferTasks = append(transferTasks, &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			updatedInfo.NamespaceId,
			updatedInfo.WorkflowId,
			updatedState.RunId,
		),
		TaskID: s.GetNextSequenceNumber(),
	})
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		RangeID: s.ShardInfo.GetRangeId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    nextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Condition:           condition,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
		},
	})
	return err
}

// UpsertChildExecutionsState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertChildExecutionsState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertChildInfos []*persistencespb.ChildExecutionInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, upsertChildInfos, nil, nil, nil,
		nil, nil, nil, nil)
}

// UpsertRequestCancelState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertRequestCancelState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertCancelInfos []*persistencespb.RequestCancelInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, upsertCancelInfos, nil,
		nil, nil, nil, nil)
}

// UpsertSignalInfoState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalInfoState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertSignalInfos []*persistencespb.SignalInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		upsertSignalInfos, nil, nil, nil)
}

// UpsertSignalsRequestedState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalsRequestedState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertSignalsRequested []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, upsertSignalsRequested, nil)
}

// DeleteChildExecutionsState is a utility method to delete child execution from mutable state
func (s *TestBase) DeleteChildExecutionsState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteChildInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, []int64{deleteChildInfo}, nil, nil,
		nil, nil, nil, nil)
}

// DeleteCancelState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteCancelState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteCancelInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, []int64{deleteCancelInfo},
		nil, nil, nil, nil)
}

// DeleteSignalState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteSignalState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteSignalInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, []int64{deleteSignalInfo}, nil, nil)
}

// DeleteSignalsRequestedState is a utility method to delete mutable state of workflow execution
func (s *TestBase) DeleteSignalsRequestedState(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteSignalsRequestedIDs []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, deleteSignalsRequestedIDs)
}

// UpdateWorklowStateAndReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorklowStateAndReplication(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, txTasks []tasks.Task) error {
	return s.UpdateWorkflowExecutionWithReplication(ctx, updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, txTasks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

// UpdateWorkflowExecutionWithRangeID is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithRangeID(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, rangeID, condition int64, timerTasks []tasks.Task,
	upsertActivityInfos []*persistencespb.ActivityInfo, deleteActivityInfos []int64, upsertTimerInfos []*persistencespb.TimerInfo,
	deleteTimerInfos []string, upsertChildInfos []*persistencespb.ChildExecutionInfo, deleteChildInfos []int64,
	upsertCancelInfos []*persistencespb.RequestCancelInfo, deleteCancelInfos []int64,
	upsertSignalInfos []*persistencespb.SignalInfo, deleteSignalInfos []int64,
	upsertSignalRequestedIDs []string, deleteSignalRequestedIDs []string) error {
	return s.UpdateWorkflowExecutionWithReplication(ctx, updatedInfo, updatedState, nextEventID, workflowTaskScheduleIDs, activityScheduleIDs, rangeID,
		condition, timerTasks, []tasks.Task{}, upsertActivityInfos, deleteActivityInfos, upsertTimerInfos, deleteTimerInfos,
		upsertChildInfos, deleteChildInfos, upsertCancelInfos, deleteCancelInfos, upsertSignalInfos, deleteSignalInfos,
		upsertSignalRequestedIDs, deleteSignalRequestedIDs)
}

// UpdateWorkflowExecutionWithReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithReplication(ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, rangeID,
	condition int64, timerTasks []tasks.Task, txTasks []tasks.Task, upsertActivityInfos []*persistencespb.ActivityInfo,
	deleteActivityInfos []int64, upsertTimerInfos []*persistencespb.TimerInfo, deleteTimerInfos []string,
	upsertChildInfos []*persistencespb.ChildExecutionInfo, deleteChildInfos []int64, upsertCancelInfos []*persistencespb.RequestCancelInfo,
	deleteCancelInfos []int64, upsertSignalInfos []*persistencespb.SignalInfo, deleteSignalInfos []int64, upsertSignalRequestedIDs []string,
	deleteSignalRequestedIDs []string) error {
	var transferTasks []tasks.Task
	var replicationTasks []tasks.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *tasks.WorkflowTask, *tasks.ActivityTask, *tasks.CloseExecutionTask, *tasks.CancelExecutionTask, *tasks.StartChildExecutionTask, *tasks.SignalExecutionTask:
			transferTasks = append(transferTasks, t)
		case *tasks.HistoryReplicationTask, *tasks.SyncActivityTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}
	workflowKey := definition.NewWorkflowKey(
		updatedInfo.NamespaceId,
		updatedInfo.WorkflowId,
		updatedState.RunId,
	)
	for _, workflowTaskScheduleID := range workflowTaskScheduleIDs {
		transferTasks = append(transferTasks, &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      s.GetNextSequenceNumber(),
			TaskQueue:   updatedInfo.TaskQueue,
			ScheduleID:  workflowTaskScheduleID})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &tasks.ActivityTask{
			WorkflowKey:       workflowKey,
			TaskID:            s.GetNextSequenceNumber(),
			TargetNamespaceID: updatedInfo.NamespaceId,
			TaskQueue:         updatedInfo.TaskQueue,
			ScheduleID:        activityScheduleID})
	}
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		RangeID: rangeID,
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    nextEventID,

			UpsertActivityInfos:       convertActivityInfos(upsertActivityInfos),
			DeleteActivityInfos:       convert.Int64SliceToSet(deleteActivityInfos),
			UpsertTimerInfos:          convertTimerInfos(upsertTimerInfos),
			DeleteTimerInfos:          convert.StringSliceToSet(deleteTimerInfos),
			UpsertChildExecutionInfos: convertChildExecutionInfos(upsertChildInfos),
			DeleteChildExecutionInfos: convert.Int64SliceToSet(deleteChildInfos),
			UpsertRequestCancelInfos:  convertRequestCancelInfos(upsertCancelInfos),
			DeleteRequestCancelInfos:  convert.Int64SliceToSet(deleteCancelInfos),
			UpsertSignalInfos:         convertSignalInfos(upsertSignalInfos),
			DeleteSignalInfos:         convert.Int64SliceToSet(deleteSignalInfos),
			UpsertSignalRequestedIDs:  convert.StringSliceToSet(upsertSignalRequestedIDs),
			DeleteSignalRequestedIDs:  convert.StringSliceToSet(deleteSignalRequestedIDs),

			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer:    transferTasks,
				tasks.CategoryTimer:       timerTasks,
				tasks.CategoryReplication: replicationTasks,
			},

			Condition: condition,
			Checksum:  testWorkflowChecksum,
		},
	})
	return err
}

// UpdateWorkflowExecutionWithTransferTasks is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithTransferTasks(
	ctx context.Context,
	updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState,
	updatedNextEventID int64, condition int64,
	transferTasks []tasks.Task,
	upsertActivityInfo []*persistencespb.ActivityInfo,
) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    updatedNextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Condition:           condition,
			UpsertActivityInfos: convertActivityInfos(upsertActivityInfo),
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForChildExecutionsInitiated is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForChildExecutionsInitiated(
	ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID int64, condition int64, transferTasks []tasks.Task, childInfos []*persistencespb.ChildExecutionInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo: updatedInfo,
			NextEventID:   updatedNextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Condition:                 condition,
			UpsertChildExecutionInfos: convertChildExecutionInfos(childInfos),
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForRequestCancel is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForRequestCancel(
	ctx context.Context, updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID int64, condition int64, transferTasks []tasks.Task,
	upsertRequestCancelInfo []*persistencespb.RequestCancelInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo: updatedInfo,
			NextEventID:   updatedNextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Condition:                condition,
			UpsertRequestCancelInfos: convertRequestCancelInfos(upsertRequestCancelInfo),
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForSignal is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForSignal(
	ctx context.Context,
	updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedNextEventID int64, condition int64,
	transferTasks []tasks.Task,
	upsertSignalInfos []*persistencespb.SignalInfo,
) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo: updatedInfo,
			NextEventID:   updatedNextEventID,
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryTransfer: transferTasks,
			},
			Condition:         condition,
			UpsertSignalInfos: convertSignalInfos(upsertSignalInfos),
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForBufferEvents is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForBufferEvents(
	ctx context.Context,
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID, condition int64,
	bufferEvents []*historypb.HistoryEvent, clearBufferedEvents bool) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			NextEventID:         updatedNextEventID,
			NewBufferedEvents:   bufferEvents,
			Condition:           condition,
			ClearBufferedEvents: clearBufferedEvents,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateAllMutableState is a utility method to update workflow execution
func (s *TestBase) UpdateAllMutableState(
	ctx context.Context,
	updatedMutableState *persistencespb.WorkflowMutableState,
	condition int64,
) error {
	var aInfos []*persistencespb.ActivityInfo
	for _, ai := range updatedMutableState.ActivityInfos {
		aInfos = append(aInfos, ai)
	}

	var tInfos []*persistencespb.TimerInfo
	for _, ti := range updatedMutableState.TimerInfos {
		tInfos = append(tInfos, ti)
	}

	var cInfos []*persistencespb.ChildExecutionInfo
	for _, ci := range updatedMutableState.ChildExecutionInfos {
		cInfos = append(cInfos, ci)
	}

	var rcInfos []*persistencespb.RequestCancelInfo
	for _, rci := range updatedMutableState.RequestCancelInfos {
		rcInfos = append(rcInfos, rci)
	}

	var sInfos []*persistencespb.SignalInfo
	for _, si := range updatedMutableState.SignalInfos {
		sInfos = append(sInfos, si)
	}

	var srIDs []string
	for _, id := range updatedMutableState.SignalRequestedIds {
		srIDs = append(srIDs, id)
	}
	_, err := s.ExecutionManager.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:             updatedMutableState.ExecutionInfo,
			NextEventID:               updatedMutableState.NextEventId,
			Condition:                 condition,
			UpsertActivityInfos:       convertActivityInfos(aInfos),
			UpsertTimerInfos:          convertTimerInfos(tInfos),
			UpsertChildExecutionInfos: convertChildExecutionInfos(cInfos),
			UpsertRequestCancelInfos:  convertRequestCancelInfos(rcInfos),
			UpsertSignalInfos:         convertSignalInfos(sInfos),
			UpsertSignalRequestedIDs:  convert.StringSliceToSet(srIDs),
		},
	})
	return err
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(ctx context.Context, info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState) error {
	return s.ExecutionManager.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     s.ShardInfo.GetShardId(),
		NamespaceID: info.NamespaceId,
		WorkflowID:  info.WorkflowId,
		RunID:       state.GetRunId(),
	})
}

// DeleteCurrentWorkflowExecution is a utility method to delete the workflow current execution
func (s *TestBase) DeleteCurrentWorkflowExecution(ctx context.Context, info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState) error {
	return s.ExecutionManager.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.ShardInfo.GetShardId(),
		NamespaceID: info.NamespaceId,
		WorkflowID:  info.WorkflowId,
		RunID:       state.GetRunId(),
	})
}

// GetTransferTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTransferTasks(ctx context.Context, batchSize int, getAll bool) ([]tasks.Task, error) {
	result := []tasks.Task{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:      s.ShardInfo.GetShardId(),
			TaskCategory: tasks.CategoryTransfer,
			InclusiveMinTaskKey: tasks.Key{
				TaskID: s.GetTransferReadLevel() + 1,
			},
			ExclusiveMaxTaskKey: tasks.Key{
				TaskID: int64(math.MaxInt64),
			},
			BatchSize:     batchSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}

		token = response.NextPageToken
		result = append(result, response.Tasks...)
		if len(token) == 0 || !getAll {
			break Loop
		}
	}

	for _, task := range result {
		atomic.StoreInt64(&s.ReadLevel, task.GetTaskID())
	}

	return result, nil
}

// GetReplicationTasks is a utility method to get tasks from replication task queue
func (s *TestBase) GetReplicationTasks(ctx context.Context, batchSize int, getAll bool) ([]tasks.Task, error) {
	result := []tasks.Task{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:      s.ShardInfo.GetShardId(),
			TaskCategory: tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.Key{
				TaskID: s.GetReplicationReadLevel() + 1,
			},
			ExclusiveMaxTaskKey: tasks.Key{
				TaskID: int64(math.MaxInt64),
			},
			BatchSize:     batchSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}

		token = response.NextPageToken
		result = append(result, response.Tasks...)
		if len(token) == 0 || !getAll {
			break Loop
		}
	}

	for _, task := range result {
		atomic.StoreInt64(&s.ReplicationReadLevel, task.GetTaskID())
	}

	return result, nil
}

func (s *TestBase) GetReplicationTasksInRange(
	ctx context.Context,
	inclusiveMinTaskID int64,
	exclusiveMaxTaskID int64,
	batchSize int,
) ([]tasks.Task, error) {
	response, err := s.ExecutionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.ShardInfo.GetShardId(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.Key{TaskID: inclusiveMinTaskID},
		ExclusiveMaxTaskKey: tasks.Key{TaskID: exclusiveMaxTaskID},
		BatchSize:           batchSize,
	})
	if err != nil {
		return nil, err
	}

	return response.Tasks, nil
}

// RangeCompleteReplicationTask is a utility method to complete a range of replication tasks
func (s *TestBase) RangeCompleteReplicationTask(ctx context.Context, exclusiveEndTaskID int64) error {
	return s.ExecutionManager.RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryReplication,
		ExclusiveMaxTaskKey: tasks.Key{
			TaskID: exclusiveEndTaskID,
		},
	})
}

// PutReplicationTaskToDLQ is a utility method to insert a replication task info
func (s *TestBase) PutReplicationTaskToDLQ(
	ctx context.Context,
	sourceCluster string,
	taskInfo *persistencespb.ReplicationTaskInfo,
) error {

	return s.ExecutionManager.PutReplicationTaskToDLQ(ctx, &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.ShardInfo.GetShardId(),
		SourceClusterName: sourceCluster,
		TaskInfo:          taskInfo,
	})
}

// GetReplicationTasksFromDLQ is a utility method to read replication task info
func (s *TestBase) GetReplicationTasksFromDLQ(
	ctx context.Context,
	sourceCluster string,
	inclusiveMinLevel int64,
	exclusiveMaxLevel int64,
	pageSize int,
	pageToken []byte,
) (*persistence.GetHistoryTasksResponse, error) {

	return s.ExecutionManager.GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             s.ShardInfo.GetShardId(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.Key{TaskID: inclusiveMinLevel},
			ExclusiveMaxTaskKey: tasks.Key{TaskID: exclusiveMaxLevel},
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: sourceCluster,
	})
}

// DeleteReplicationTaskFromDLQ is a utility method to delete a replication task info
func (s *TestBase) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	sourceCluster string,
	taskID int64,
) error {

	return s.ExecutionManager.DeleteReplicationTaskFromDLQ(ctx, &persistence.DeleteReplicationTaskFromDLQRequest{
		CompleteHistoryTaskRequest: persistence.CompleteHistoryTaskRequest{
			ShardID:      s.ShardInfo.GetShardId(),
			TaskCategory: tasks.CategoryReplication,
			TaskKey:      tasks.Key{TaskID: taskID},
		},
		SourceClusterName: sourceCluster,
	})
}

// RangeDeleteReplicationTaskFromDLQ is a utility method to delete  replication task info
func (s *TestBase) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	sourceCluster string,
	inclusiveMinTaskID int64,
	exclusiveMaxTaskID int64,
) error {

	return s.ExecutionManager.RangeDeleteReplicationTaskFromDLQ(ctx, &persistence.RangeDeleteReplicationTaskFromDLQRequest{
		RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             s.ShardInfo.GetShardId(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.Key{TaskID: inclusiveMinTaskID},
			ExclusiveMaxTaskKey: tasks.Key{TaskID: exclusiveMaxTaskID},
		},
		SourceClusterName: sourceCluster,
	})
}

// CompleteTransferTask is a utility method to complete a transfer task
func (s *TestBase) CompleteTransferTask(ctx context.Context, taskID int64) error {

	return s.ExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryTransfer,
		TaskKey: tasks.Key{
			TaskID: taskID,
		},
	})
}

// RangeCompleteTransferTask is a utility method to complete a range of transfer tasks
func (s *TestBase) RangeCompleteTransferTask(ctx context.Context, inclusiveMinTaskID int64, exclusiveMaxTaskID int64) error {
	return s.ExecutionManager.RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.Key{
			TaskID: inclusiveMinTaskID,
		},
		ExclusiveMaxTaskKey: tasks.Key{
			TaskID: exclusiveMaxTaskID,
		},
	})
}

// CompleteReplicationTask is a utility method to complete a replication task
func (s *TestBase) CompleteReplicationTask(ctx context.Context, taskID int64) error {

	return s.ExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryReplication,
		TaskKey: tasks.Key{
			TaskID: taskID,
		},
	})
}

// GetTimerTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTimerTasks(ctx context.Context, batchSize int, getAll bool) ([]tasks.Task, error) {
	result := []tasks.Task{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:      s.ShardInfo.GetShardId(),
			TaskCategory: tasks.CategoryTimer,
			InclusiveMinTaskKey: tasks.Key{
				FireTime: time.Unix(0, 0).UTC(),
			},
			ExclusiveMaxTaskKey: tasks.Key{
				FireTime: time.Unix(0, math.MaxInt64).UTC(),
			},
			BatchSize:     batchSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}

		token = response.NextPageToken
		result = append(result, response.Tasks...)
		if len(token) == 0 || !getAll {
			break Loop
		}
	}

	return result, nil
}

// CompleteTimerTask is a utility method to complete a timer task
func (s *TestBase) CompleteTimerTask(ctx context.Context, ts time.Time, taskID int64) error {
	return s.ExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryTimer,
		TaskKey: tasks.Key{
			FireTime: ts,
			TaskID:   taskID,
		},
	})
}

func (s *TestBase) CompleteTimerTaskProto(ctx context.Context, ts *types.Timestamp, taskID int64) error {
	t, err := types.TimestampFromProto(ts)
	if err != nil {
		return err
	}
	return s.CompleteTimerTask(ctx, t, taskID)
}

// RangeCompleteTimerTask is a utility method to complete a range of timer tasks
func (s *TestBase) RangeCompleteTimerTask(ctx context.Context, inclusiveBeginTimestamp time.Time, exclusiveEndTimestamp time.Time) error {
	return s.ExecutionManager.RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:      s.ShardInfo.GetShardId(),
		TaskCategory: tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.Key{
			FireTime: inclusiveBeginTimestamp,
		},
		ExclusiveMaxTaskKey: tasks.Key{
			FireTime: exclusiveEndTimestamp,
		},
	})
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	s.TaskMgr.Close()
	s.ClusterMetadataManager.Close()
	s.MetadataManager.Close()
	s.ExecutionManager.Close()
	s.ShardMgr.Close()
	s.ExecutionManager.Close()
	s.NamespaceReplicationQueue.Stop()
	s.Factory.Close()
	s.DefaultTestCluster.TearDownTestDatabase()
}

// GetNextSequenceNumber generates a unique sequence number for can be used for transfer queue taskId
func (s *TestBase) GetNextSequenceNumber() int64 {
	taskID, _ := s.TaskIDGenerator.GenerateTransferTaskID()
	return taskID
}

// GetTransferReadLevel returns the current read level for shard
func (s *TestBase) GetTransferReadLevel() int64 {
	return atomic.LoadInt64(&s.ReadLevel)
}

// GetReplicationReadLevel returns the current read level for shard
func (s *TestBase) GetReplicationReadLevel() int64 {
	return atomic.LoadInt64(&s.ReplicationReadLevel)
}

// ClearTasks completes all transfer tasks and replication tasks
func (s *TestBase) ClearTasks(ctx context.Context) {
	s.ClearTransferQueue(ctx)
	s.ClearReplicationQueue(ctx)
}

// ClearTransferQueue completes all tasks in transfer queue
func (s *TestBase) ClearTransferQueue(ctx context.Context) {
	s.Logger.Info("Clearing transfer tasks", tag.ShardRangeID(s.ShardInfo.GetRangeId()), tag.ReadLevel(s.GetTransferReadLevel()))
	tasks, err := s.GetTransferTasks(ctx, 100, true)
	if err != nil {
		s.Logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.Logger.Info("Deleting transfer task with ID", tag.TaskID(t.GetTaskID()))
		s.NoError(s.CompleteTransferTask(ctx, t.GetTaskID()))
		counter++
	}

	s.Logger.Info("Deleted transfer tasks.", tag.Counter(counter))
	atomic.StoreInt64(&s.ReadLevel, 0)
}

// ClearReplicationQueue completes all tasks in replication queue
func (s *TestBase) ClearReplicationQueue(ctx context.Context) {
	s.Logger.Info("Clearing replication tasks", tag.ShardRangeID(s.ShardInfo.GetRangeId()), tag.ReadLevel(s.GetReplicationReadLevel()))
	tasks, err := s.GetReplicationTasks(ctx, 100, true)
	if err != nil {
		s.Logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.Logger.Info("Deleting replication task with ID", tag.TaskID(t.GetTaskID()))
		s.NoError(s.CompleteReplicationTask(ctx, t.GetTaskID()))
		counter++
	}

	s.Logger.Info("Deleted replication tasks.", tag.Counter(counter))
	atomic.StoreInt64(&s.ReplicationReadLevel, 0)
}

// EqualTimesWithPrecision assertion that two times are equal within precision
func (s *TestBase) EqualTimesWithPrecision(t1, t2 time.Time, precision time.Duration) {
	s.True(timeComparatorGo(t1, t2, precision),
		"Not equal: \n"+
			"expected: %s\n"+
			"actual  : %s%s", t1, t2,
	)
}

// EqualTimes assertion that two times are equal within two millisecond precision
func (s *TestBase) EqualTimes(t1, t2 time.Time) {
	s.EqualTimesWithPrecision(t1, t2, TimePrecision)
}

func (s *TestBase) validateTimeRange(t time.Time, expectedDuration time.Duration) bool {
	currentTime := time.Now().UTC()
	diff := time.Duration(currentTime.UnixNano() - t.UnixNano())
	if diff > expectedDuration {
		s.Logger.Info("Check Current time, Application time, Differenrce", tag.Timestamp(t), tag.CursorTimestamp(currentTime), tag.Number(int64(diff)))
		return false
	}
	return true
}

// GenerateTransferTaskID helper
func (g *TestTransferTaskIDGenerator) GenerateTransferTaskID() (int64, error) {
	return atomic.AddInt64(&g.seqNum, 1), nil
}

// Publish is a utility method to add messages to the queue
func (s *TestBase) Publish(
	ctx context.Context,
	message interface{},
) error {

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.Retry(
		func() error {
			return s.NamespaceReplicationQueue.Publish(ctx, message)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

func isMessageIDConflictError(err error) bool {
	_, ok := err.(*persistence.ConditionFailedError)
	return ok
}

// GetReplicationMessages is a utility method to get messages from the queue
func (s *TestBase) GetReplicationMessages(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	return s.NamespaceReplicationQueue.GetReplicationMessages(ctx, lastMessageID, pageSize)
}

// UpdateAckLevel updates replication queue ack level
func (s *TestBase) UpdateAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
	clusterName string,
) error {

	return s.NamespaceReplicationQueue.UpdateAckLevel(ctx, lastProcessedMessageID, clusterName)
}

// GetAckLevels returns replication queue ack levels
func (s *TestBase) GetAckLevels(
	ctx context.Context,
) (map[string]int64, error) {
	return s.NamespaceReplicationQueue.GetAckLevels(ctx)
}

// PublishToNamespaceDLQ is a utility method to add messages to the namespace DLQ
func (s *TestBase) PublishToNamespaceDLQ(
	ctx context.Context,
	message interface{},
) error {

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.RetryContext(
		ctx,
		func(ctx context.Context) error {
			return s.NamespaceReplicationQueue.PublishToDLQ(ctx, message)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

// GetMessagesFromNamespaceDLQ is a utility method to get messages from the namespace DLQ
func (s *TestBase) GetMessagesFromNamespaceDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	return s.NamespaceReplicationQueue.GetMessagesFromDLQ(
		ctx,
		firstMessageID,
		lastMessageID,
		pageSize,
		pageToken,
	)
}

// UpdateNamespaceDLQAckLevel updates namespace dlq ack level
func (s *TestBase) UpdateNamespaceDLQAckLevel(
	ctx context.Context,
	lastProcessedMessageID int64,
) error {

	return s.NamespaceReplicationQueue.UpdateDLQAckLevel(ctx, lastProcessedMessageID)
}

// GetNamespaceDLQAckLevel returns namespace dlq ack level
func (s *TestBase) GetNamespaceDLQAckLevel(
	ctx context.Context,
) (int64, error) {
	return s.NamespaceReplicationQueue.GetDLQAckLevel(ctx)
}

// DeleteMessageFromNamespaceDLQ deletes one message from namespace DLQ
func (s *TestBase) DeleteMessageFromNamespaceDLQ(
	ctx context.Context,
	messageID int64,
) error {

	return s.NamespaceReplicationQueue.DeleteMessageFromDLQ(ctx, messageID)
}

// RangeDeleteMessagesFromNamespaceDLQ deletes messages from namespace DLQ
func (s *TestBase) RangeDeleteMessagesFromNamespaceDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {

	return s.NamespaceReplicationQueue.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}

// GenerateTransferTaskIDs helper
func (g *TestTransferTaskIDGenerator) GenerateTransferTaskIDs(number int) ([]int64, error) {
	result := []int64{}
	for i := 0; i < number; i++ {
		id, err := g.GenerateTransferTaskID()
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}

func randString(length int) string {
	const lowercaseSet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, length)
	for i := range b {
		b[i] = lowercaseSet[rand.Int63()%int64(len(lowercaseSet))]
	}
	return string(b)
}

// GenerateRandomDBName helper
// Format: MMDDHHMMSS_abc
func GenerateRandomDBName(n int) string {
	now := time.Now().UTC()
	rand.Seed(now.UnixNano())
	var prefix strings.Builder
	prefix.WriteString(now.Format("0102150405"))
	prefix.WriteRune('_')
	prefix.WriteString(randString(n))
	return prefix.String()
}

func convertActivityInfos(
	inputs []*persistencespb.ActivityInfo,
) map[int64]*persistencespb.ActivityInfo {

	outputs := make(map[int64]*persistencespb.ActivityInfo, len(inputs))
	for _, item := range inputs {
		outputs[item.ScheduleId] = item
	}
	return outputs
}

func convertTimerInfos(
	inputs []*persistencespb.TimerInfo,
) map[string]*persistencespb.TimerInfo {

	outputs := make(map[string]*persistencespb.TimerInfo, len(inputs))
	for _, item := range inputs {
		outputs[item.TimerId] = item
	}
	return outputs
}

func convertChildExecutionInfos(
	inputs []*persistencespb.ChildExecutionInfo,
) map[int64]*persistencespb.ChildExecutionInfo {

	outputs := make(map[int64]*persistencespb.ChildExecutionInfo, len(inputs))
	for _, item := range inputs {
		outputs[item.InitiatedId] = item
	}
	return outputs
}

func convertRequestCancelInfos(
	inputs []*persistencespb.RequestCancelInfo,
) map[int64]*persistencespb.RequestCancelInfo {

	outputs := make(map[int64]*persistencespb.RequestCancelInfo, len(inputs))
	for _, item := range inputs {
		outputs[item.InitiatedId] = item
	}
	return outputs
}

func convertSignalInfos(
	inputs []*persistencespb.SignalInfo,
) map[int64]*persistencespb.SignalInfo {

	outputs := make(map[int64]*persistencespb.SignalInfo, len(inputs))
	for _, item := range inputs {
		outputs[item.InitiatedId] = item
	}
	return outputs
}

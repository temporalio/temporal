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
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/environment"
)

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GenerateTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		SQLDBPluginName string
		DBName          string
		DBUsername      string
		DBPassword      string
		DBHost          string
		DBPort          int              `yaml:"-"`
		StoreType       string           `yaml:"-"`
		SchemaDir       string           `yaml:"-"`
		ClusterMetadata cluster.Metadata `yaml:"-"`
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr                  persistence.ShardManager
		AbstractDataStoreFactory  client.AbstractDataStoreFactory
		ExecutionMgrFactory       client.Factory
		ExecutionManager          persistence.ExecutionManager
		TaskMgr                   persistence.TaskManager
		HistoryV2Mgr              persistence.HistoryManager
		ClusterMetadataManager    persistence.ClusterMetadataManager
		MetadataManager           persistence.MetadataManager
		VisibilityMgr             persistence.VisibilityManager
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		ShardInfo                 *persistencespb.ShardInfo
		TaskIDGenerator           TransferTaskIDGenerator
		ClusterMetadata           cluster.Metadata
		ReadLevel                 int64
		ReplicationReadLevel      int64
		DefaultTestCluster        PersistenceTestCluster
		VisibilityTestCluster     PersistenceTestCluster
		logger                    log.Logger
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
	logger, err := loggerimpl.NewDevelopment()
	if err != nil {
		panic(err)
	}
	testCluster := cassandra.NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, logger)
	return newTestBase(options, testCluster, logger)
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(3)
	}
	logger, err := loggerimpl.NewDevelopment()
	if err != nil {
		panic(err)
	}

	if options.DBPort == 0 {
		switch options.SQLDBPluginName {
		case mysql.PluginName:
			options.DBPort = environment.GetMySQLPort()
		case postgresql.PluginName:
			options.DBPort = environment.GetPostgreSQLPort()
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
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", options.SQLDBPluginName))
		}
	}
	testCluster := sql.NewTestCluster(options.SQLDBPluginName, options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, logger)
	return newTestBase(options, testCluster, logger)
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

func newTestBase(options *TestBaseOptions, testCluster PersistenceTestCluster, logger log.Logger) TestBase {
	metadata := options.ClusterMetadata
	if metadata == nil {
		metadata = cluster.GetTestClusterMetadata(false, false)
	}
	options.ClusterMetadata = metadata
	base := TestBase{
		DefaultTestCluster:    testCluster,
		VisibilityTestCluster: testCluster,
		ClusterMetadata:       metadata,
	}
	base.logger = logger
	return base
}

// Config returns the persistence configuration for this test
func (s *TestBase) Config() config.Persistence {
	cfg := s.DefaultTestCluster.Config()
	if s.DefaultTestCluster == s.VisibilityTestCluster {
		return cfg
	}
	vCfg := s.VisibilityTestCluster.Config()
	cfg.VisibilityStore = "visibility_ " + vCfg.VisibilityStore
	cfg.DataStores[cfg.VisibilityStore] = vCfg.DataStores[vCfg.VisibilityStore]
	return cfg
}

// Setup sets up the test base, must be called as part of SetupSuite
func (s *TestBase) Setup() {
	var err error
	shardID := int32(10)
	clusterName := s.ClusterMetadata.GetCurrentClusterName()

	s.DefaultTestCluster.SetupTestDatabase()
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		s.VisibilityTestCluster.SetupTestDatabase()
	}

	cfg := s.DefaultTestCluster.Config()
	scope := tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
	metricsClient := metrics.NewClient(scope, metrics.GetMetricsServiceIdx(common.HistoryServiceName, s.logger))
	factory := client.NewFactory(&cfg, nil, s.AbstractDataStoreFactory, clusterName, metricsClient, s.logger)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.ClusterMetadataManager, err = factory.NewClusterMetadataManager()
	s.fatalOnError("NewClusterMetadataManager", err)

	s.MetadataManager, err = factory.NewMetadataManager()
	s.fatalOnError("NewMetadataManager", err)

	s.HistoryV2Mgr, err = factory.NewHistoryManager()
	s.fatalOnError("NewHistoryManager", err)

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionMgrFactory = factory
	s.ExecutionManager, err = factory.NewExecutionManager(shardID)
	s.fatalOnError("NewExecutionManager", err)

	visibilityFactory := factory
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		vCfg := s.VisibilityTestCluster.Config()
		visibilityFactory = client.NewFactory(&vCfg, nil, nil, clusterName, nil, s.logger)
	}
	// SQL currently doesn't have support for visibility manager
	s.VisibilityMgr, err = visibilityFactory.NewVisibilityManager()
	if err != nil {
		s.fatalOnError("NewVisibilityManager", err)
	}

	s.ReadLevel = 0
	s.ReplicationReadLevel = 0
	s.ShardInfo = &persistencespb.ShardInfo{
		ShardId:                 shardID,
		RangeId:                 0,
		TransferAckLevel:        0,
		ReplicationAckLevel:     0,
		TimerAckLevelTime:       &time.Time{},
		ClusterTimerAckLevel:    map[string]*time.Time{},
		ClusterTransferAckLevel: map[string]int64{clusterName: 0},
	}

	s.TaskIDGenerator = &TestTransferTaskIDGenerator{}
	err = s.ShardMgr.CreateShard(&persistence.CreateShardRequest{ShardInfo: s.ShardInfo})
	s.fatalOnError("CreateShard", err)

	queue, err := factory.NewNamespaceReplicationQueue()
	s.fatalOnError("Create NamespaceReplicationQueue", err)
	s.NamespaceReplicationQueue = queue
}

func (s *TestBase) fatalOnError(msg string, err error) {
	if err != nil {
		s.logger.Fatal(msg, tag.Error(err))
	}
}

// CreateShard is a utility method to create the shard using persistence layer
func (s *TestBase) CreateShard(shardID int32, owner string, rangeID int64) error {
	info := &persistencespb.ShardInfo{
		ShardId: shardID,
		Owner:   owner,
		RangeId: rangeID,
	}

	return s.ShardMgr.CreateShard(&persistence.CreateShardRequest{
		ShardInfo: info,
	})
}

// GetShard is a utility method to get the shard using persistence layer
func (s *TestBase) GetShard(shardID int32) (*persistencespb.ShardInfo, error) {
	response, err := s.ShardMgr.GetShard(&persistence.GetShardRequest{
		ShardID: shardID,
	})

	if err != nil {
		return nil, err
	}

	return response.ShardInfo, nil
}

// UpdateShard is a utility method to update the shard using persistence layer
func (s *TestBase) UpdateShard(updatedInfo *persistencespb.ShardInfo, previousRangeID int64) error {
	return s.ShardMgr.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedInfo,
		PreviousRangeID: previousRangeID,
	})
}

// CreateWorkflowExecutionWithBranchToken test util function
func (s *TestBase) CreateWorkflowExecutionWithBranchToken(namespaceID string, workflowExecution commonpb.WorkflowExecution, taskQueue,
	wType string, wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleID int64, branchToken []byte, timerTasks []persistence.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskQueue,
				WorkflowTypeName:           wType,
				WorkflowRunTimeout:         wTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastProcessedEvent:         lastProcessedEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				EventBranchToken:           branchToken,
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: nextEventID,
			TransferTasks: []persistence.Task{
				&persistence.WorkflowTask{
					TaskID:              s.GetNextSequenceNumber(),
					NamespaceID:         namespaceID,
					TaskQueue:           taskQueue,
					ScheduleID:          workflowTaskScheduleID,
					VisibilityTimestamp: time.Now().UTC(),
				},
			},
			TimerTasks: timerTasks,
			Checksum:   testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(namespaceID string, workflowExecution commonpb.WorkflowExecution, taskQueue, wType string, wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID, lastProcessedEventID, workflowTaskScheduleID int64, timerTasks []persistence.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	return s.CreateWorkflowExecutionWithBranchToken(namespaceID, workflowExecution, taskQueue, wType, wTimeout, workflowTaskTimeout,
		nextEventID, lastProcessedEventID, workflowTaskScheduleID, nil, timerTasks)
}

// CreateWorkflowExecutionManyTasks is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionManyTasks(namespaceID string, workflowExecution commonpb.WorkflowExecution,
	taskQueue string, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64) (*persistence.CreateWorkflowExecutionResponse, error) {

	transferTasks := []persistence.Task{}
	for _, workflowTaskScheduleID := range workflowTaskScheduleIDs {
		transferTasks = append(transferTasks,
			&persistence.WorkflowTask{
				TaskID:      s.GetNextSequenceNumber(),
				NamespaceID: namespaceID,
				TaskQueue:   taskQueue,
				ScheduleID:  workflowTaskScheduleID,
			})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks,
			&persistence.ActivityTask{
				TaskID:      s.GetNextSequenceNumber(),
				NamespaceID: namespaceID,
				TaskQueue:   taskQueue,
				ScheduleID:  activityScheduleID,
			})
	}

	response, err := s.ExecutionManager.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:            namespaceID,
				WorkflowId:             workflowExecution.GetWorkflowId(),
				TaskQueue:              taskQueue,
				LastFirstEventId:       common.FirstEventID,
				LastProcessedEvent:     lastProcessedEventID,
				WorkflowTaskScheduleId: common.EmptyEventID,
				WorkflowTaskStartedId:  common.EmptyEventID,
				WorkflowTaskTimeout:    timestamp.DurationFromSeconds(1),
				ExecutionStats:         &persistencespb.ExecutionStats{},
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			TransferTasks: transferTasks,
			Checksum:      testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// CreateChildWorkflowExecution is a utility method to create child workflow executions
func (s *TestBase) CreateChildWorkflowExecution(namespaceID string, workflowExecution commonpb.WorkflowExecution,
	parentNamespaceID string, parentExecution commonpb.WorkflowExecution, initiatedID int64, taskQueue, wType string,
	wTimeout *time.Duration, workflowTaskTimeout *time.Duration, nextEventID int64, lastProcessedEventID int64,
	workflowTaskScheduleID int64, timerTasks []persistence.Task) (*persistence.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
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
				LastProcessedEvent:         lastProcessedEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: nextEventID,
			TransferTasks: []persistence.Task{
				&persistence.WorkflowTask{
					TaskID:      s.GetNextSequenceNumber(),
					NamespaceID: namespaceID,
					TaskQueue:   taskQueue,
					ScheduleID:  workflowTaskScheduleID,
				},
			},
			TimerTasks: timerTasks,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	return response, err
}

// GetWorkflowExecutionInfoWithStats is a utility method to retrieve execution info with size stats
func (s *TestBase) GetWorkflowExecutionInfoWithStats(namespaceID string, workflowExecution commonpb.WorkflowExecution) (
	*persistence.MutableStateStats, *persistencespb.WorkflowMutableState, error) {
	response, err := s.ExecutionManager.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution:   workflowExecution,
	})
	if err != nil {
		return nil, nil, err
	}

	return response.MutableStateStats, response.State, nil
}

// GetWorkflowExecutionInfo is a utility method to retrieve execution info
func (s *TestBase) GetWorkflowMutableState(namespaceID string, workflowExecution commonpb.WorkflowExecution) (
	*persistencespb.WorkflowMutableState, error) {
	response, err := s.ExecutionManager.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution:   workflowExecution,
	})
	if err != nil {
		return nil, err
	}
	return response.State, nil
}

// GetCurrentWorkflowRunID returns the workflow run ID for the given params
func (s *TestBase) GetCurrentWorkflowRunID(namespaceID, workflowID string) (string, error) {
	response, err := s.ExecutionManager.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})

	if err != nil {
		return "", err
	}

	return response.RunID, nil
}

// ContinueAsNewExecution is a utility method to create workflow executions
func (s *TestBase) ContinueAsNewExecution(updatedInfo *persistencespb.WorkflowExecutionInfo, updatedState *persistencespb.WorkflowExecutionState, updatedNextEventID int64, condition int64,
	newExecution commonpb.WorkflowExecution, nextEventID, workflowTaskScheduleID int64,
	prevResetPoints *workflowpb.ResetPoints) error {
	newworkflowTask := &persistence.WorkflowTask{
		TaskID:      s.GetNextSequenceNumber(),
		NamespaceID: updatedInfo.NamespaceId,
		TaskQueue:   updatedInfo.TaskQueue,
		ScheduleID:  workflowTaskScheduleID,
	}

	req := &persistence.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionState:      updatedState,
			NextEventID:         updatedNextEventID,
			TransferTasks:       []persistence.Task{newworkflowTask},
			TimerTasks:          nil,
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
				LastProcessedEvent:         common.EmptyEventID,
				WorkflowTaskScheduleId:     workflowTaskScheduleID,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				AutoResetPoints:            prevResetPoints,
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           newExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           updatedState.State,
				Status:          updatedState.Status,
			},
			TransferTasks: nil,
			TimerTasks:    nil,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	}
	req.UpdateWorkflowMutation.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	req.UpdateWorkflowMutation.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	_, err := s.ExecutionManager.UpdateWorkflowExecution(req)
	return err
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, condition int64, timerTasks []persistence.Task,
	upsertActivityInfos []*persistencespb.ActivityInfo, deleteActivityInfos []int64,
	upsertTimerInfos []*persistencespb.TimerInfo, deleteTimerInfos []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, workflowTaskScheduleIDs, activityScheduleIDs,
		s.ShardInfo.GetRangeId(), condition, timerTasks, upsertActivityInfos, deleteActivityInfos,
		upsertTimerInfos, deleteTimerInfos, nil, nil, nil, nil,
		nil, nil, nil, nil)
}

// UpdateWorkflowExecutionAndFinish is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionAndFinish(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64, condition int64) error {
	var transferTasks []persistence.Task
	transferTasks = append(transferTasks, &persistence.CloseExecutionTask{TaskID: s.GetNextSequenceNumber()})
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionState:      updatedState,
			NextEventID:         nextEventID,
			TransferTasks:       transferTasks,
			TimerTasks:          nil,
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
func (s *TestBase) UpsertChildExecutionsState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertChildInfos []*persistencespb.ChildExecutionInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, upsertChildInfos, nil, nil, nil,
		nil, nil, nil, nil)
}

// UpsertRequestCancelState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertRequestCancelState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertCancelInfos []*persistencespb.RequestCancelInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, upsertCancelInfos, nil,
		nil, nil, nil, nil)
}

// UpsertSignalInfoState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalInfoState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertSignalInfos []*persistencespb.SignalInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		upsertSignalInfos, nil, nil, nil)
}

// UpsertSignalsRequestedState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalsRequestedState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, upsertSignalsRequested []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, upsertSignalsRequested, nil)
}

// DeleteChildExecutionsState is a utility method to delete child execution from mutable state
func (s *TestBase) DeleteChildExecutionsState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteChildInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, []int64{deleteChildInfo}, nil, nil,
		nil, nil, nil, nil)
}

// DeleteCancelState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteCancelState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteCancelInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, []int64{deleteCancelInfo},
		nil, nil, nil, nil)
}

// DeleteSignalState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteSignalState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteSignalInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, []int64{deleteSignalInfo}, nil, nil)
}

// DeleteSignalsRequestedState is a utility method to delete mutable state of workflow execution
func (s *TestBase) DeleteSignalsRequestedState(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, deleteSignalsRequestedIDs []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, deleteSignalsRequestedIDs)
}

// UpdateWorklowStateAndReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorklowStateAndReplication(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	condition int64, txTasks []persistence.Task) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedState, nextEventID, nil, nil,
		s.ShardInfo.GetRangeId(), condition, nil, txTasks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

// UpdateWorkflowExecutionWithRangeID is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithRangeID(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, rangeID, condition int64, timerTasks []persistence.Task,
	upsertActivityInfos []*persistencespb.ActivityInfo, deleteActivityInfos []int64, upsertTimerInfos []*persistencespb.TimerInfo,
	deleteTimerInfos []string, upsertChildInfos []*persistencespb.ChildExecutionInfo, deleteChildInfos []int64,
	upsertCancelInfos []*persistencespb.RequestCancelInfo, deleteCancelInfos []int64,
	upsertSignalInfos []*persistencespb.SignalInfo, deleteSignalInfos []int64,
	upsertSignalRequestedIDs []string, deleteSignalRequestedIDs []string) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedState, nextEventID, workflowTaskScheduleIDs, activityScheduleIDs, rangeID,
		condition, timerTasks, []persistence.Task{}, upsertActivityInfos, deleteActivityInfos, upsertTimerInfos, deleteTimerInfos,
		upsertChildInfos, deleteChildInfos, upsertCancelInfos, deleteCancelInfos, upsertSignalInfos, deleteSignalInfos,
		upsertSignalRequestedIDs, deleteSignalRequestedIDs)
}

// UpdateWorkflowExecutionWithReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithReplication(updatedInfo *persistencespb.WorkflowExecutionInfo,
	updatedState *persistencespb.WorkflowExecutionState, nextEventID int64,
	workflowTaskScheduleIDs []int64, activityScheduleIDs []int64, rangeID,
	condition int64, timerTasks []persistence.Task, txTasks []persistence.Task, upsertActivityInfos []*persistencespb.ActivityInfo,
	deleteActivityInfos []int64, upsertTimerInfos []*persistencespb.TimerInfo, deleteTimerInfos []string,
	upsertChildInfos []*persistencespb.ChildExecutionInfo, deleteChildInfos []int64, upsertCancelInfos []*persistencespb.RequestCancelInfo,
	deleteCancelInfos []int64, upsertSignalInfos []*persistencespb.SignalInfo, deleteSignalInfos []int64, upsertSignalRequestedIDs []string,
	deleteSignalRequestedIDs []string) error {
	var transferTasks []persistence.Task
	var replicationTasks []persistence.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *persistence.WorkflowTask, *persistence.ActivityTask, *persistence.CloseExecutionTask, *persistence.CancelExecutionTask, *persistence.StartChildExecutionTask, *persistence.SignalExecutionTask, *persistence.RecordWorkflowStartedTask:
			transferTasks = append(transferTasks, t)
		case *persistence.HistoryReplicationTask, *persistence.SyncActivityTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}
	for _, workflowTaskScheduleID := range workflowTaskScheduleIDs {
		transferTasks = append(transferTasks, &persistence.WorkflowTask{
			TaskID:      s.GetNextSequenceNumber(),
			NamespaceID: updatedInfo.NamespaceId,
			TaskQueue:   updatedInfo.TaskQueue,
			ScheduleID:  workflowTaskScheduleID})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &persistence.ActivityTask{
			TaskID:      s.GetNextSequenceNumber(),
			NamespaceID: updatedInfo.NamespaceId,
			TaskQueue:   updatedInfo.TaskQueue,
			ScheduleID:  activityScheduleID})
	}
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		RangeID: rangeID,
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    nextEventID,

			UpsertActivityInfos:       upsertActivityInfos,
			DeleteActivityInfos:       deleteActivityInfos,
			UpsertTimerInfos:          upsertTimerInfos,
			DeleteTimerInfos:          deleteTimerInfos,
			UpsertChildExecutionInfos: upsertChildInfos,
			DeleteChildExecutionInfos: deleteChildInfos,
			UpsertRequestCancelInfos:  upsertCancelInfos,
			DeleteRequestCancelInfos:  deleteCancelInfos,
			UpsertSignalInfos:         upsertSignalInfos,
			DeleteSignalInfos:         deleteSignalInfos,
			UpsertSignalRequestedIDs:  upsertSignalRequestedIDs,
			DeleteSignalRequestedIDs:  deleteSignalRequestedIDs,

			TransferTasks:    transferTasks,
			ReplicationTasks: replicationTasks,
			TimerTasks:       timerTasks,

			Condition: condition,
			Checksum:  testWorkflowChecksum,
		},
	})
	return err
}

// UpdateWorkflowExecutionWithTransferTasks is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithTransferTasks(
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedState *persistencespb.WorkflowExecutionState, updatedNextEventID int64, condition int64, transferTasks []persistence.Task, upsertActivityInfo []*persistencespb.ActivityInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionState:      updatedState,
			NextEventID:         updatedNextEventID,
			TransferTasks:       transferTasks,
			Condition:           condition,
			UpsertActivityInfos: upsertActivityInfo,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForChildExecutionsInitiated is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForChildExecutionsInitiated(
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID int64, condition int64, transferTasks []persistence.Task, childInfos []*persistencespb.ChildExecutionInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:             updatedInfo,
			NextEventID:               updatedNextEventID,
			TransferTasks:             transferTasks,
			Condition:                 condition,
			UpsertChildExecutionInfos: childInfos,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForRequestCancel is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForRequestCancel(
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID int64, condition int64, transferTasks []persistence.Task,
	upsertRequestCancelInfo []*persistencespb.RequestCancelInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:            updatedInfo,
			NextEventID:              updatedNextEventID,
			TransferTasks:            transferTasks,
			Condition:                condition,
			UpsertRequestCancelInfos: upsertRequestCancelInfo,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForSignal is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForSignal(
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID int64, condition int64, transferTasks []persistence.Task,
	upsertSignalInfos []*persistencespb.SignalInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:     updatedInfo,
			NextEventID:       updatedNextEventID,
			TransferTasks:     transferTasks,
			Condition:         condition,
			UpsertSignalInfos: upsertSignalInfos,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})
	return err
}

// UpdateWorkflowExecutionForBufferEvents is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForBufferEvents(
	updatedInfo *persistencespb.WorkflowExecutionInfo, updatedNextEventID, condition int64,
	bufferEvents []*historypb.HistoryEvent, clearBufferedEvents bool) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
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
func (s *TestBase) UpdateAllMutableState(updatedMutableState *persistencespb.WorkflowMutableState, condition int64) error {
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
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:             updatedMutableState.ExecutionInfo,
			NextEventID:               updatedMutableState.NextEventId,
			Condition:                 condition,
			UpsertActivityInfos:       aInfos,
			UpsertTimerInfos:          tInfos,
			UpsertChildExecutionInfos: cInfos,
			UpsertRequestCancelInfos:  rcInfos,
			UpsertSignalInfos:         sInfos,
			UpsertSignalRequestedIDs:  srIDs,
		},
	})
	return err
}

// ConflictResolveWorkflowExecution is  utility method to reset mutable state
func (s *TestBase) ConflictResolveWorkflowExecution(prevRunID string, prevLastWriteVersion int64, prevState enumsspb.WorkflowExecutionState,
	info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState, nextEventID int64,
	activityInfos []*persistencespb.ActivityInfo, timerInfos []*persistencespb.TimerInfo, childExecutionInfos []*persistencespb.ChildExecutionInfo,
	requestCancelInfos []*persistencespb.RequestCancelInfo, signalInfos []*persistencespb.SignalInfo, ids []string) error {
	return s.ExecutionManager.ConflictResolveWorkflowExecution(&persistence.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),
		ResetWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo:       info,
			Condition:           nextEventID,
			ActivityInfos:       activityInfos,
			TimerInfos:          timerInfos,
			ChildExecutionInfos: childExecutionInfos,
			RequestCancelInfos:  requestCancelInfos,
			SignalInfos:         signalInfos,
			SignalRequestedIDs:  ids,
			Checksum:            testWorkflowChecksum,
		},
	})
}

// ResetWorkflowExecution is  utility method to reset WF
func (s *TestBase) ResetWorkflowExecution(condition int64, executionInfo *persistencespb.WorkflowExecutionInfo, executionState *persistencespb.WorkflowExecutionState, nextEventID int64,
	activityInfos []*persistencespb.ActivityInfo, timerInfos []*persistencespb.TimerInfo, childExecutionInfos []*persistencespb.ChildExecutionInfo,
	requestCancelInfos []*persistencespb.RequestCancelInfo, signalInfos []*persistencespb.SignalInfo, ids []string, trasTasks, timerTasks, replTasks []persistence.Task,
	updateCurr bool, currInfo *persistencespb.WorkflowExecutionInfo, currExecutionState *persistencespb.WorkflowExecutionState, curNextEventID int64,
	currTrasTasks, currTimerTasks []persistence.Task, forkRunID string, forkRunNextEventID int64) error {

	req := &persistence.ResetWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),

		BaseRunID:          forkRunID,
		BaseRunNextEventID: forkRunNextEventID,

		CurrentRunID:          currExecutionState.GetRunId(),
		CurrentRunNextEventID: condition,

		CurrentWorkflowMutation: nil,

		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo:  executionInfo,
			ExecutionState: executionState,
			NextEventID:    nextEventID,

			ActivityInfos:       activityInfos,
			TimerInfos:          timerInfos,
			ChildExecutionInfos: childExecutionInfos,
			RequestCancelInfos:  requestCancelInfos,
			SignalInfos:         signalInfos,
			SignalRequestedIDs:  ids,

			TransferTasks:    trasTasks,
			ReplicationTasks: replTasks,
			TimerTasks:       timerTasks,
		},
	}

	if updateCurr {
		req.CurrentWorkflowMutation = &persistence.WorkflowMutation{
			ExecutionInfo: currInfo,
			NextEventID:   curNextEventID,

			TransferTasks: currTrasTasks,
			TimerTasks:    currTimerTasks,

			Condition: condition,
		}
	}

	return s.ExecutionManager.ResetWorkflowExecution(req)
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState) error {
	return s.ExecutionManager.DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
		NamespaceID: info.NamespaceId,
		WorkflowID:  info.WorkflowId,
		RunID:       state.GetRunId(),
	})
}

// DeleteCurrentWorkflowExecution is a utility method to delete the workflow current execution
func (s *TestBase) DeleteCurrentWorkflowExecution(info *persistencespb.WorkflowExecutionInfo, state *persistencespb.WorkflowExecutionState) error {
	return s.ExecutionManager.DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
		NamespaceID: info.NamespaceId,
		WorkflowID:  info.WorkflowId,
		RunID:       state.GetRunId(),
	})
}

// GetTransferTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTransferTasks(batchSize int, getAll bool) ([]*persistencespb.TransferTaskInfo, error) {
	result := []*persistencespb.TransferTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetTransferTasks(&persistence.GetTransferTasksRequest{
			ReadLevel:     s.GetTransferReadLevel(),
			MaxReadLevel:  int64(math.MaxInt64),
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
		atomic.StoreInt64(&s.ReadLevel, task.GetTaskId())
	}

	return result, nil
}

// GetReplicationTasks is a utility method to get tasks from replication task queue
func (s *TestBase) GetReplicationTasks(batchSize int, getAll bool) ([]*persistencespb.ReplicationTaskInfo, error) {
	result := []*persistencespb.ReplicationTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetReplicationTasks(&persistence.GetReplicationTasksRequest{
			ReadLevel:     s.GetReplicationReadLevel(),
			MaxReadLevel:  int64(math.MaxInt64),
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
		atomic.StoreInt64(&s.ReplicationReadLevel, task.GetTaskId())
	}

	return result, nil
}

// RangeCompleteReplicationTask is a utility method to complete a range of replication tasks
func (s *TestBase) RangeCompleteReplicationTask(inclusiveEndTaskID int64) error {
	return s.ExecutionManager.RangeCompleteReplicationTask(&persistence.RangeCompleteReplicationTaskRequest{
		InclusiveEndTaskID: inclusiveEndTaskID,
	})
}

// PutReplicationTaskToDLQ is a utility method to insert a replication task info
func (s *TestBase) PutReplicationTaskToDLQ(
	sourceCluster string,
	taskInfo *persistencespb.ReplicationTaskInfo,
) error {

	return s.ExecutionManager.PutReplicationTaskToDLQ(&persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: sourceCluster,
		TaskInfo:          taskInfo,
	})
}

// GetReplicationTasksFromDLQ is a utility method to read replication task info
func (s *TestBase) GetReplicationTasksFromDLQ(
	sourceCluster string,
	readLevel int64,
	maxReadLevel int64,
	pageSize int,
	pageToken []byte,
) (*persistence.GetReplicationTasksFromDLQResponse, error) {

	return s.ExecutionManager.GetReplicationTasksFromDLQ(&persistence.GetReplicationTasksFromDLQRequest{
		SourceClusterName: sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			ReadLevel:     readLevel,
			MaxReadLevel:  maxReadLevel,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	})
}

// DeleteReplicationTaskFromDLQ is a utility method to delete a replication task info
func (s *TestBase) DeleteReplicationTaskFromDLQ(
	sourceCluster string,
	taskID int64,
) error {

	return s.ExecutionManager.DeleteReplicationTaskFromDLQ(&persistence.DeleteReplicationTaskFromDLQRequest{
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	})
}

// RangeDeleteReplicationTaskFromDLQ is a utility method to delete  replication task info
func (s *TestBase) RangeDeleteReplicationTaskFromDLQ(
	sourceCluster string,
	beginTaskID int64,
	endTaskID int64,
) error {

	return s.ExecutionManager.RangeDeleteReplicationTaskFromDLQ(&persistence.RangeDeleteReplicationTaskFromDLQRequest{
		SourceClusterName:    sourceCluster,
		ExclusiveBeginTaskID: beginTaskID,
		InclusiveEndTaskID:   endTaskID,
	})
}

// CompleteTransferTask is a utility method to complete a transfer task
func (s *TestBase) CompleteTransferTask(taskID int64) error {

	return s.ExecutionManager.CompleteTransferTask(&persistence.CompleteTransferTaskRequest{
		TaskID: taskID,
	})
}

// RangeCompleteTransferTask is a utility method to complete a range of transfer tasks
func (s *TestBase) RangeCompleteTransferTask(exclusiveBeginTaskID int64, inclusiveEndTaskID int64) error {
	return s.ExecutionManager.RangeCompleteTransferTask(&persistence.RangeCompleteTransferTaskRequest{
		ExclusiveBeginTaskID: exclusiveBeginTaskID,
		InclusiveEndTaskID:   inclusiveEndTaskID,
	})
}

// CompleteReplicationTask is a utility method to complete a replication task
func (s *TestBase) CompleteReplicationTask(taskID int64) error {

	return s.ExecutionManager.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{
		TaskID: taskID,
	})
}

// GetTimerIndexTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTimerIndexTasks(batchSize int, getAll bool) ([]*persistencespb.TimerTaskInfo, error) {
	result := []*persistencespb.TimerTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetTimerIndexTasks(&persistence.GetTimerIndexTasksRequest{
			MinTimestamp:  time.Time{},
			MaxTimestamp:  time.Unix(0, math.MaxInt64).UTC(),
			BatchSize:     batchSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}

		token = response.NextPageToken
		result = append(result, response.Timers...)
		if len(token) == 0 || !getAll {
			break Loop
		}
	}

	return result, nil
}

// CompleteTimerTask is a utility method to complete a timer task
func (s *TestBase) CompleteTimerTask(ts time.Time, taskID int64) error {
	return s.ExecutionManager.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{
		VisibilityTimestamp: ts,
		TaskID:              taskID,
	})
}

func (s *TestBase) CompleteTimerTaskProto(ts *types.Timestamp, taskID int64) error {
	t, err := types.TimestampFromProto(ts)
	if err != nil {
		return err
	}
	return s.CompleteTimerTask(t, taskID)
}

// RangeCompleteTimerTask is a utility method to complete a range of timer tasks
func (s *TestBase) RangeCompleteTimerTask(inclusiveBeginTimestamp time.Time, exclusiveEndTimestamp time.Time) error {
	return s.ExecutionManager.RangeCompleteTimerTask(&persistence.RangeCompleteTimerTaskRequest{
		InclusiveBeginTimestamp: inclusiveBeginTimestamp,
		ExclusiveEndTimestamp:   exclusiveEndTimestamp,
	})
}

// CreateWorkflowTask is a utility method to create a task
func (s *TestBase) CreateWorkflowTask(namespaceID string, workflowExecution commonpb.WorkflowExecution, taskQueue string,
	workflowTaskScheduleID int64) (int64, error) {
	leaseResponse, err := s.TaskMgr.LeaseTaskQueue(&persistence.LeaseTaskQueueRequest{
		NamespaceID: namespaceID,
		TaskQueue:   taskQueue,
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	if err != nil {
		return 0, err
	}

	taskID := s.GetNextSequenceNumber()
	tasks := []*persistencespb.AllocatedTaskInfo{
		{
			TaskId: taskID,
			Data: &persistencespb.TaskInfo{
				NamespaceId: namespaceID,
				WorkflowId:  workflowExecution.WorkflowId,
				RunId:       workflowExecution.RunId,
				ScheduleId:  workflowTaskScheduleID,
				CreateTime:  timestamp.TimeNowPtrUtc(),
			},
		},
	}

	_, err = s.TaskMgr.CreateTasks(&persistence.CreateTasksRequest{
		TaskQueueInfo: leaseResponse.TaskQueueInfo,
		Tasks:         tasks,
	})

	if err != nil {
		return 0, err
	}

	return taskID, err
}

// CreateActivityTasks is a utility method to create tasks
func (s *TestBase) CreateActivityTasks(namespaceID string, workflowExecution commonpb.WorkflowExecution,
	activities map[int64]string) ([]int64, error) {

	taskQueues := make(map[string]*persistence.PersistedTaskQueueInfo)
	for _, tl := range activities {
		_, ok := taskQueues[tl]
		if !ok {
			resp, err := s.TaskMgr.LeaseTaskQueue(
				&persistence.LeaseTaskQueueRequest{NamespaceID: namespaceID, TaskQueue: tl, TaskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY})
			if err != nil {
				return []int64{}, err
			}
			taskQueues[tl] = resp.TaskQueueInfo
		}
	}

	var taskIDs []int64
	for activityScheduleID, taskQueue := range activities {
		taskID := s.GetNextSequenceNumber()
		tasks := []*persistencespb.AllocatedTaskInfo{
			{
				Data: &persistencespb.TaskInfo{
					NamespaceId: namespaceID,
					WorkflowId:  workflowExecution.WorkflowId,
					RunId:       workflowExecution.RunId,
					ScheduleId:  activityScheduleID,
					ExpiryTime:  timestamp.TimeNowPtrUtcAddSeconds(defaultScheduleToStartTimeout),
					CreateTime:  timestamp.TimeNowPtrUtc(),
				},
				TaskId: taskID,
			},
		}
		_, err := s.TaskMgr.CreateTasks(&persistence.CreateTasksRequest{
			TaskQueueInfo: taskQueues[taskQueue],
			Tasks:         tasks,
		})
		if err != nil {
			return nil, err
		}
		taskIDs = append(taskIDs, taskID)
	}

	return taskIDs, nil
}

// GetTasks is a utility method to get tasks from persistence
func (s *TestBase) GetTasks(namespaceID string, taskQueue string, taskType enumspb.TaskQueueType, batchSize int) (*persistence.GetTasksResponse, error) {
	response, err := s.TaskMgr.GetTasks(&persistence.GetTasksRequest{
		NamespaceID:  namespaceID,
		TaskQueue:    taskQueue,
		TaskType:     taskType,
		BatchSize:    batchSize,
		MaxReadLevel: convert.Int64Ptr(math.MaxInt64),
	})

	if err != nil {
		return nil, err
	}

	return &persistence.GetTasksResponse{Tasks: response.Tasks}, nil
}

// CompleteTask is a utility method to complete a task
func (s *TestBase) CompleteTask(namespaceID string, taskQueue string, taskType enumspb.TaskQueueType, taskID int64) error {
	return s.TaskMgr.CompleteTask(&persistence.CompleteTaskRequest{
		TaskQueue: &persistence.TaskQueueKey{
			NamespaceID: namespaceID,
			TaskType:    taskType,
			Name:        taskQueue,
		},
		TaskID: taskID,
	})
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	s.ExecutionMgrFactory.Close()
	// TODO VisibilityMgr/Store is created with a separated code path, this is incorrect and may cause leaking connection
	// And Postgres requires all connection to be closed before dropping a database
	// https://github.com/uber/cadence/issues/2854
	// Remove the below line after the issue is fix
	s.VisibilityMgr.Close()

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
func (s *TestBase) ClearTasks() {
	s.ClearTransferQueue()
	s.ClearReplicationQueue()
}

// ClearTransferQueue completes all tasks in transfer queue
func (s *TestBase) ClearTransferQueue() {
	s.logger.Info("Clearing transfer tasks", tag.ShardRangeID(s.ShardInfo.GetRangeId()), tag.ReadLevel(s.GetTransferReadLevel()))
	tasks, err := s.GetTransferTasks(100, true)
	if err != nil {
		s.logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.logger.Info("Deleting transfer task with ID", tag.TaskID(t.GetTaskId()))
		s.NoError(s.CompleteTransferTask(t.GetTaskId()))
		counter++
	}

	s.logger.Info("Deleted transfer tasks.", tag.Counter(counter))
	atomic.StoreInt64(&s.ReadLevel, 0)
}

// ClearReplicationQueue completes all tasks in replication queue
func (s *TestBase) ClearReplicationQueue() {
	s.logger.Info("Clearing replication tasks", tag.ShardRangeID(s.ShardInfo.GetRangeId()), tag.ReadLevel(s.GetReplicationReadLevel()))
	tasks, err := s.GetReplicationTasks(100, true)
	if err != nil {
		s.logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.logger.Info("Deleting replication task with ID", tag.TaskID(t.GetTaskId()))
		s.NoError(s.CompleteReplicationTask(t.GetTaskId()))
		counter++
	}

	s.logger.Info("Deleted replication tasks.", tag.Counter(counter))
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
		s.logger.Info("Check Current time, Application time, Differenrce", tag.Timestamp(t), tag.CursorTimestamp(currentTime), tag.Number(int64(diff)))
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
	message interface{},
) error {

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.Retry(
		func() error {
			return s.NamespaceReplicationQueue.Publish(message)
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
	lastMessageID int64,
	pageSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	return s.NamespaceReplicationQueue.GetReplicationMessages(lastMessageID, pageSize)
}

// UpdateAckLevel updates replication queue ack level
func (s *TestBase) UpdateAckLevel(
	lastProcessedMessageID int64,
	clusterName string,
) error {

	return s.NamespaceReplicationQueue.UpdateAckLevel(lastProcessedMessageID, clusterName)
}

// GetAckLevels returns replication queue ack levels
func (s *TestBase) GetAckLevels() (map[string]int64, error) {
	return s.NamespaceReplicationQueue.GetAckLevels()
}

// PublishToNamespaceDLQ is a utility method to add messages to the namespace DLQ
func (s *TestBase) PublishToNamespaceDLQ(
	message interface{},
) error {

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.Retry(
		func() error {
			return s.NamespaceReplicationQueue.PublishToDLQ(message)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

// GetMessagesFromNamespaceDLQ is a utility method to get messages from the namespace DLQ
func (s *TestBase) GetMessagesFromNamespaceDLQ(
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	return s.NamespaceReplicationQueue.GetMessagesFromDLQ(
		firstMessageID,
		lastMessageID,
		pageSize,
		pageToken,
	)
}

// UpdateNamespaceDLQAckLevel updates namespace dlq ack level
func (s *TestBase) UpdateNamespaceDLQAckLevel(
	lastProcessedMessageID int64,
) error {

	return s.NamespaceReplicationQueue.UpdateDLQAckLevel(lastProcessedMessageID)
}

// GetNamespaceDLQAckLevel returns namespace dlq ack level
func (s *TestBase) GetNamespaceDLQAckLevel() (int64, error) {
	return s.NamespaceReplicationQueue.GetDLQAckLevel()
}

// DeleteMessageFromNamespaceDLQ deletes one message from namespace DLQ
func (s *TestBase) DeleteMessageFromNamespaceDLQ(
	messageID int64,
) error {

	return s.NamespaceReplicationQueue.DeleteMessageFromDLQ(messageID)
}

// RangeDeleteMessagesFromNamespaceDLQ deletes messages from namespace DLQ
func (s *TestBase) RangeDeleteMessagesFromNamespaceDLQ(
	firstMessageID int64,
	lastMessageID int64,
) error {

	return s.NamespaceReplicationQueue.RangeDeleteMessagesFromDLQ(firstMessageID, lastMessageID)
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

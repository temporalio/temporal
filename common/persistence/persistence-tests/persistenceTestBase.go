// Copyright (c) 2017 Uber Technologies, Inc.
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
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/replicator"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	pfactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/service/config"
)

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GenerateTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		DBName          string
		DBPort          int              `yaml:"-"`
		StoreType       string           `yaml:"-"`
		SchemaDir       string           `yaml:"-"`
		ClusterMetadata cluster.Metadata `yaml:"-"`
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr               p.ShardManager
		ExecutionMgrFactory    pfactory.Factory
		ExecutionManager       p.ExecutionManager
		TaskMgr                p.TaskManager
		HistoryV2Mgr           p.HistoryV2Manager
		MetadataManager        p.MetadataManager
		VisibilityMgr          p.VisibilityManager
		DomainReplicationQueue p.DomainReplicationQueue
		ShardInfo              *p.ShardInfo
		TaskIDGenerator        TransferTaskIDGenerator
		ClusterMetadata        cluster.Metadata
		ReadLevel              int64
		ReplicationReadLevel   int64
		DefaultTestCluster     PersistenceTestCluster
		VisibilityTestCluster  PersistenceTestCluster
		logger                 log.Logger
	}

	// PersistenceTestCluster exposes management operations on a database
	PersistenceTestCluster interface {
		DatabaseName() string
		SetupTestDatabase()
		TearDownTestDatabase()
		CreateSession()
		DropDatabase()
		Config() config.Persistence
		LoadSchema(fileNames []string, schemaDir string)
		LoadVisibilitySchema(fileNames []string, schemaDir string)
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
		options.DBName = "test_" + GenerateRandomDBName(10)
	}
	testCluster := cassandra.NewTestCluster(options.DBName, options.DBPort, options.SchemaDir)
	return newTestBase(options, testCluster)
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = GenerateRandomDBName(10)
	}
	testCluster := sql.NewTestCluster(options.DBName, options.DBPort, options.SchemaDir)
	return newTestBase(options, testCluster)
}

// NewTestBase returns a persistence test base backed by either cassandra or sql
func NewTestBase(options *TestBaseOptions) TestBase {
	switch options.StoreType {
	case config.StoreTypeSQL:
		return NewTestBaseWithSQL(options)
	case config.StoreTypeCassandra:
		return NewTestBaseWithCassandra(options)
	default:
		panic("invalid storeType " + options.StoreType)
	}
}

func newTestBase(options *TestBaseOptions, testCluster PersistenceTestCluster) TestBase {
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
	logger, err := loggerimpl.NewDevelopment()
	if err != nil {
		panic(err)
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
	shardID := 10
	clusterName := s.ClusterMetadata.GetCurrentClusterName()

	s.DefaultTestCluster.SetupTestDatabase()
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		s.VisibilityTestCluster.SetupTestDatabase()
	}

	cfg := s.DefaultTestCluster.Config()
	factory := pfactory.New(&cfg, clusterName, nil, s.logger)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.MetadataManager, err = factory.NewMetadataManager()
	s.fatalOnError("NewMetadataManager", err)

	s.HistoryV2Mgr, err = factory.NewHistoryV2Manager()
	s.fatalOnError("NewHistoryV2Manager", err)

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionMgrFactory = factory
	s.ExecutionManager, err = factory.NewExecutionManager(shardID)
	s.fatalOnError("NewExecutionManager", err)

	visibilityFactory := factory
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		vCfg := s.VisibilityTestCluster.Config()
		visibilityFactory = pfactory.New(&vCfg, clusterName, nil, s.logger)
	}
	// SQL currently doesn't have support for visibility manager
	s.VisibilityMgr, err = visibilityFactory.NewVisibilityManager()
	if err != nil {
		s.fatalOnError("NewVisibilityManager", err)
	}

	s.ReadLevel = 0
	s.ReplicationReadLevel = 0
	s.ShardInfo = &p.ShardInfo{
		ShardID:                 shardID,
		RangeID:                 0,
		TransferAckLevel:        0,
		ReplicationAckLevel:     0,
		TimerAckLevel:           time.Time{},
		ClusterTimerAckLevel:    map[string]time.Time{clusterName: time.Time{}},
		ClusterTransferAckLevel: map[string]int64{clusterName: 0},
	}

	s.TaskIDGenerator = &TestTransferTaskIDGenerator{}
	err = s.ShardMgr.CreateShard(&p.CreateShardRequest{ShardInfo: s.ShardInfo})
	s.fatalOnError("CreateShard", err)

	queue, err := factory.NewDomainReplicationQueue()
	s.fatalOnError("Create DomainReplicationQueue", err)
	s.DomainReplicationQueue = queue
}

func (s *TestBase) fatalOnError(msg string, err error) {
	if err != nil {
		s.logger.Fatal(msg, tag.Error(err))
	}
}

// CreateShard is a utility method to create the shard using persistence layer
func (s *TestBase) CreateShard(shardID int, owner string, rangeID int64) error {
	info := &p.ShardInfo{
		ShardID: shardID,
		Owner:   owner,
		RangeID: rangeID,
	}

	return s.ShardMgr.CreateShard(&p.CreateShardRequest{
		ShardInfo: info,
	})
}

// GetShard is a utility method to get the shard using persistence layer
func (s *TestBase) GetShard(shardID int) (*p.ShardInfo, error) {
	response, err := s.ShardMgr.GetShard(&p.GetShardRequest{
		ShardID: shardID,
	})

	if err != nil {
		return nil, err
	}

	return response.ShardInfo, nil
}

// UpdateShard is a utility method to update the shard using persistence layer
func (s *TestBase) UpdateShard(updatedInfo *p.ShardInfo, previousRangeID int64) error {
	return s.ShardMgr.UpdateShard(&p.UpdateShardRequest{
		ShardInfo:       updatedInfo,
		PreviousRangeID: previousRangeID,
	})
}

// CreateWorkflowExecutionWithBranchToken test util function
func (s *TestBase) CreateWorkflowExecutionWithBranchToken(domainID string, workflowExecution workflow.WorkflowExecution, taskList,
	wType string, wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, branchToken []byte, timerTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             taskList,
				WorkflowTypeName:     wType,
				WorkflowTimeout:      wTimeout,
				DecisionTimeoutValue: decisionTimeout,
				ExecutionContext:     executionContext,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
				DecisionScheduleID:   decisionScheduleID,
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
				BranchToken:          branchToken,
			},
			ExecutionStats: &p.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.DecisionTask{
					TaskID:              s.GetNextSequenceNumber(),
					DomainID:            domainID,
					TaskList:            taskList,
					ScheduleID:          decisionScheduleID,
					VisibilityTimestamp: time.Now(),
				},
			},
			TimerTasks: timerTasks,
		},
		RangeID: s.ShardInfo.RangeID,
	})

	return response, err
}

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(domainID string, workflowExecution workflow.WorkflowExecution, taskList,
	wType string, wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, timerTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	return s.CreateWorkflowExecutionWithBranchToken(domainID, workflowExecution, taskList, wType, wTimeout, decisionTimeout,
		executionContext, nextEventID, lastProcessedEventID, decisionScheduleID, nil, timerTasks)
}

// CreateWorkflowExecutionWithReplication is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionWithReplication(domainID string, workflowExecution workflow.WorkflowExecution,
	taskList, wType string, wTimeout int32, decisionTimeout int32, nextEventID int64,
	lastProcessedEventID int64, decisionScheduleID int64, state *p.ReplicationState, txTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	var transferTasks []p.Task
	var replicationTasks []p.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *p.DecisionTask, *p.ActivityTask, *p.CloseExecutionTask, *p.CancelExecutionTask, *p.StartChildExecutionTask, *p.SignalExecutionTask, *p.RecordWorkflowStartedTask:
			transferTasks = append(transferTasks, t)
		case *p.HistoryReplicationTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}

	transferTasks = append(transferTasks, &p.DecisionTask{
		TaskID:     s.GetNextSequenceNumber(),
		DomainID:   domainID,
		TaskList:   taskList,
		ScheduleID: decisionScheduleID,
	})
	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             taskList,
				WorkflowTypeName:     wType,
				WorkflowTimeout:      wTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
				DecisionScheduleID:   decisionScheduleID,
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
			},
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: state,
			TransferTasks:    transferTasks,
			ReplicationTasks: replicationTasks,
		},
		RangeID: s.ShardInfo.RangeID,
	})

	return response, err
}

// CreateWorkflowExecutionManyTasks is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionManyTasks(domainID string, workflowExecution workflow.WorkflowExecution,
	taskList string, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleIDs []int64, activityScheduleIDs []int64) (*p.CreateWorkflowExecutionResponse, error) {

	transferTasks := []p.Task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks,
			&p.DecisionTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: int64(decisionScheduleID),
			})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks,
			&p.ActivityTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: int64(activityScheduleID),
			})
	}

	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:    uuid.New(),
				DomainID:           domainID,
				WorkflowID:         workflowExecution.GetWorkflowId(),
				RunID:              workflowExecution.GetRunId(),
				TaskList:           taskList,
				ExecutionContext:   executionContext,
				State:              p.WorkflowStateRunning,
				CloseStatus:        p.WorkflowCloseStatusNone,
				LastFirstEventID:   common.FirstEventID,
				NextEventID:        nextEventID,
				LastProcessedEvent: lastProcessedEventID,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
				DecisionTimeout:    1,
			},
			ExecutionStats: &p.ExecutionStats{},
			TransferTasks:  transferTasks,
		},
		RangeID: s.ShardInfo.RangeID,
	})

	return response, err
}

// CreateChildWorkflowExecution is a utility method to create child workflow executions
func (s *TestBase) CreateChildWorkflowExecution(domainID string, workflowExecution workflow.WorkflowExecution,
	parentDomainID string, parentExecution workflow.WorkflowExecution, initiatedID int64, taskList, wType string,
	wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, timerTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				ParentDomainID:       parentDomainID,
				ParentWorkflowID:     parentExecution.GetWorkflowId(),
				ParentRunID:          parentExecution.GetRunId(),
				InitiatedID:          initiatedID,
				TaskList:             taskList,
				WorkflowTypeName:     wType,
				WorkflowTimeout:      wTimeout,
				DecisionTimeoutValue: decisionTimeout,
				ExecutionContext:     executionContext,
				State:                p.WorkflowStateCreated,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
				DecisionScheduleID:   decisionScheduleID,
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
			},
			ExecutionStats: &p.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.DecisionTask{
					TaskID:     s.GetNextSequenceNumber(),
					DomainID:   domainID,
					TaskList:   taskList,
					ScheduleID: decisionScheduleID,
				},
			},
			TimerTasks: timerTasks,
		},
		RangeID: s.ShardInfo.RangeID,
	})

	return response, err
}

// GetWorkflowExecutionInfoWithStats is a utility method to retrieve execution info with size stats
func (s *TestBase) GetWorkflowExecutionInfoWithStats(domainID string, workflowExecution workflow.WorkflowExecution) (
	*p.MutableStateStats, *p.WorkflowMutableState, error) {
	response, err := s.ExecutionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		DomainID:  domainID,
		Execution: workflowExecution,
	})
	if err != nil {
		return nil, nil, err
	}

	return response.MutableStateStats, response.State, nil
}

// GetWorkflowExecutionInfo is a utility method to retrieve execution info
func (s *TestBase) GetWorkflowExecutionInfo(domainID string, workflowExecution workflow.WorkflowExecution) (
	*p.WorkflowMutableState, error) {
	response, err := s.ExecutionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		DomainID:  domainID,
		Execution: workflowExecution,
	})
	if err != nil {
		return nil, err
	}
	return response.State, nil
}

// GetCurrentWorkflowRunID returns the workflow run ID for the given params
func (s *TestBase) GetCurrentWorkflowRunID(domainID, workflowID string) (string, error) {
	response, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})

	if err != nil {
		return "", err
	}

	return response.RunID, nil
}

// ContinueAsNewExecution is a utility method to create workflow executions
func (s *TestBase) ContinueAsNewExecution(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64,
	newExecution workflow.WorkflowExecution, nextEventID, decisionScheduleID int64,
	prevResetPoints *workflow.ResetPoints) error {
	return s.ContinueAsNewExecutionWithReplication(
		updatedInfo, updatedStats, condition, newExecution, nextEventID, decisionScheduleID, prevResetPoints, nil, nil,
	)
}

// ContinueAsNewExecutionWithReplication is a utility method to create workflow executions
func (s *TestBase) ContinueAsNewExecutionWithReplication(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64,
	newExecution workflow.WorkflowExecution, nextEventID, decisionScheduleID int64,
	prevResetPoints *workflow.ResetPoints, beforeState *p.ReplicationState, afterState *p.ReplicationState) error {
	newdecisionTask := &p.DecisionTask{
		TaskID:     s.GetNextSequenceNumber(),
		DomainID:   updatedInfo.DomainID,
		TaskList:   updatedInfo.TaskList,
		ScheduleID: int64(decisionScheduleID),
	}

	req := &p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			TransferTasks:       []p.Task{newdecisionTask},
			TimerTasks:          nil,
			Condition:           condition,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
			ReplicationState:    beforeState,
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             updatedInfo.DomainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             updatedInfo.TaskList,
				WorkflowTypeName:     updatedInfo.WorkflowTypeName,
				WorkflowTimeout:      updatedInfo.WorkflowTimeout,
				DecisionTimeoutValue: updatedInfo.DecisionTimeoutValue,
				ExecutionContext:     nil,
				State:                updatedInfo.State,
				CloseStatus:          updatedInfo.CloseStatus,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   common.EmptyEventID,
				DecisionScheduleID:   decisionScheduleID,
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
				AutoResetPoints:      prevResetPoints,
			},
			ExecutionStats:   updatedStats,
			ReplicationState: afterState,
			TransferTasks:    nil,
			TimerTasks:       nil,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	}
	req.UpdateWorkflowMutation.ExecutionInfo.State = p.WorkflowStateCompleted
	req.UpdateWorkflowMutation.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusContinuedAsNew
	_, err := s.ExecutionManager.UpdateWorkflowExecution(req)
	return err
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	decisionScheduleIDs []int64, activityScheduleIDs []int64, condition int64, timerTasks []p.Task,
	upsertActivityInfos []*p.ActivityInfo, deleteActivityInfos []int64,
	upsertTimerInfos []*p.TimerInfo, deleteTimerInfos []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, decisionScheduleIDs, activityScheduleIDs,
		s.ShardInfo.RangeID, condition, timerTasks, upsertActivityInfos, deleteActivityInfos,
		upsertTimerInfos, deleteTimerInfos, nil, nil, nil, nil,
		nil, nil, nil, "")
}

// UpdateWorkflowExecutionAndFinish is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionAndFinish(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64) error {
	transferTasks := []p.Task{}
	transferTasks = append(transferTasks, &p.CloseExecutionTask{TaskID: s.GetNextSequenceNumber()})
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			TransferTasks:       transferTasks,
			TimerTasks:          nil,
			Condition:           condition,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
		},
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpsertChildExecutionsState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertChildExecutionsState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, upsertChildInfos []*p.ChildExecutionInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, upsertChildInfos, nil, nil, nil,
		nil, nil, nil, "")
}

// UpsertRequestCancelState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertRequestCancelState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, upsertCancelInfos []*p.RequestCancelInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, upsertCancelInfos, nil,
		nil, nil, nil, "")
}

// UpsertSignalInfoState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalInfoState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, upsertSignalInfos []*p.SignalInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		upsertSignalInfos, nil, nil, "")
}

// UpsertSignalsRequestedState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalsRequestedState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, upsertSignalsRequested []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, upsertSignalsRequested, "")
}

// DeleteChildExecutionsState is a utility method to delete child execution from mutable state
func (s *TestBase) DeleteChildExecutionsState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, deleteChildInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, &deleteChildInfo, nil, nil,
		nil, nil, nil, "")
}

// DeleteCancelState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteCancelState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, deleteCancelInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, nil, &deleteCancelInfo,
		nil, nil, nil, "")
}

// DeleteSignalState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteSignalState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, deleteSignalInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, &deleteSignalInfo, nil, "")
}

// DeleteSignalsRequestedState is a utility method to delete mutable state of workflow execution
func (s *TestBase) DeleteSignalsRequestedState(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	condition int64, deleteSignalsRequestedID string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, updatedStats, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, deleteSignalsRequestedID)
}

// UpdateWorklowStateAndReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorklowStateAndReplication(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats,
	updatedReplicationState *p.ReplicationState, updatedVersionHistories *p.VersionHistories,
	condition int64, txTasks []p.Task) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedStats, updatedReplicationState, updatedVersionHistories, nil, nil,
		s.ShardInfo.RangeID, condition, nil, txTasks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "",
	)
}

// UpdateWorkflowExecutionWithRangeID is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithRangeID(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, updatedVersionHistories *p.VersionHistories,
	decisionScheduleIDs []int64, activityScheduleIDs []int64, rangeID, condition int64, timerTasks []p.Task,
	upsertActivityInfos []*p.ActivityInfo, deleteActivityInfos []int64, upsertTimerInfos []*p.TimerInfo,
	deleteTimerInfos []string, upsertChildInfos []*p.ChildExecutionInfo, deleteChildInfo *int64,
	upsertCancelInfos []*p.RequestCancelInfo, deleteCancelInfo *int64,
	upsertSignalInfos []*p.SignalInfo, deleteSignalInfo *int64,
	upsertSignalRequestedIDs []string, deleteSignalRequestedID string) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedStats, nil, updatedVersionHistories, decisionScheduleIDs, activityScheduleIDs, rangeID,
		condition, timerTasks, []p.Task{}, upsertActivityInfos, deleteActivityInfos, upsertTimerInfos, deleteTimerInfos,
		upsertChildInfos, deleteChildInfo, upsertCancelInfos, deleteCancelInfo, upsertSignalInfos, deleteSignalInfo,
		upsertSignalRequestedIDs, deleteSignalRequestedID)
}

// UpdateWorkflowExecutionWithReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithReplication(updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats,
	updatedReplicationState *p.ReplicationState, updatedVersionHistories *p.VersionHistories, decisionScheduleIDs []int64, activityScheduleIDs []int64, rangeID,
	condition int64, timerTasks []p.Task, txTasks []p.Task, upsertActivityInfos []*p.ActivityInfo,
	deleteActivityInfos []int64, upsertTimerInfos []*p.TimerInfo, deleteTimerInfos []string,
	upsertChildInfos []*p.ChildExecutionInfo, deleteChildInfo *int64, upsertCancelInfos []*p.RequestCancelInfo,
	deleteCancelInfo *int64, upsertSignalInfos []*p.SignalInfo, deleteSignalInfo *int64, upsertSignalRequestedIDs []string,
	deleteSignalRequestedID string) error {
	var transferTasks []p.Task
	var replicationTasks []p.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *p.DecisionTask, *p.ActivityTask, *p.CloseExecutionTask, *p.CancelExecutionTask, *p.StartChildExecutionTask, *p.SignalExecutionTask, *p.RecordWorkflowStartedTask:
			transferTasks = append(transferTasks, t)
		case *p.HistoryReplicationTask, *p.SyncActivityTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &p.DecisionTask{
			TaskID:     s.GetNextSequenceNumber(),
			DomainID:   updatedInfo.DomainID,
			TaskList:   updatedInfo.TaskList,
			ScheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &p.ActivityTask{
			TaskID:     s.GetNextSequenceNumber(),
			DomainID:   updatedInfo.DomainID,
			TaskList:   updatedInfo.TaskList,
			ScheduleID: int64(activityScheduleID)})
	}
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		RangeID: rangeID,
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:    updatedInfo,
			ExecutionStats:   updatedStats,
			ReplicationState: updatedReplicationState,
			VersionHistories: updatedVersionHistories,

			UpsertActivityInfos:       upsertActivityInfos,
			DeleteActivityInfos:       deleteActivityInfos,
			UpsertTimerInfos:          upsertTimerInfos,
			DeleteTimerInfos:          deleteTimerInfos,
			UpsertChildExecutionInfos: upsertChildInfos,
			DeleteChildExecutionInfo:  deleteChildInfo,
			UpsertRequestCancelInfos:  upsertCancelInfos,
			DeleteRequestCancelInfo:   deleteCancelInfo,
			UpsertSignalInfos:         upsertSignalInfos,
			DeleteSignalInfo:          deleteSignalInfo,
			UpsertSignalRequestedIDs:  upsertSignalRequestedIDs,
			DeleteSignalRequestedID:   deleteSignalRequestedID,

			TransferTasks:    transferTasks,
			ReplicationTasks: replicationTasks,
			TimerTasks:       timerTasks,

			Condition: condition,
		},
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionWithTransferTasks is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithTransferTasks(
	updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64, transferTasks []p.Task, upsertActivityInfo []*p.ActivityInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			TransferTasks:       transferTasks,
			Condition:           condition,
			UpsertActivityInfos: upsertActivityInfo,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForChildExecutionsInitiated is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForChildExecutionsInitiated(
	updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64, transferTasks []p.Task, childInfos []*p.ChildExecutionInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:             updatedInfo,
			ExecutionStats:            updatedStats,
			TransferTasks:             transferTasks,
			Condition:                 condition,
			UpsertChildExecutionInfos: childInfos,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForRequestCancel is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForRequestCancel(
	updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64, transferTasks []p.Task,
	upsertRequestCancelInfo []*p.RequestCancelInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:            updatedInfo,
			ExecutionStats:           updatedStats,
			TransferTasks:            transferTasks,
			Condition:                condition,
			UpsertRequestCancelInfos: upsertRequestCancelInfo,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForSignal is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForSignal(
	updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, condition int64, transferTasks []p.Task,
	upsertSignalInfos []*p.SignalInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:     updatedInfo,
			ExecutionStats:    updatedStats,
			TransferTasks:     transferTasks,
			Condition:         condition,
			UpsertSignalInfos: upsertSignalInfos,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForBufferEvents is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForBufferEvents(
	updatedInfo *p.WorkflowExecutionInfo, updatedStats *p.ExecutionStats, rState *p.ReplicationState, condition int64,
	bufferEvents []*workflow.HistoryEvent, clearBufferedEvents bool) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			ReplicationState:    rState,
			NewBufferedEvents:   bufferEvents,
			Condition:           condition,
			ClearBufferedEvents: clearBufferedEvents,
		},
		RangeID:  s.ShardInfo.RangeID,
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateAllMutableState is a utility method to update workflow execution
func (s *TestBase) UpdateAllMutableState(updatedMutableState *p.WorkflowMutableState, condition int64) error {
	var aInfos []*p.ActivityInfo
	for _, ai := range updatedMutableState.ActivityInfos {
		aInfos = append(aInfos, ai)
	}

	var tInfos []*p.TimerInfo
	for _, ti := range updatedMutableState.TimerInfos {
		tInfos = append(tInfos, ti)
	}

	var cInfos []*p.ChildExecutionInfo
	for _, ci := range updatedMutableState.ChildExecutionInfos {
		cInfos = append(cInfos, ci)
	}

	var rcInfos []*p.RequestCancelInfo
	for _, rci := range updatedMutableState.RequestCancelInfos {
		rcInfos = append(rcInfos, rci)
	}

	var sInfos []*p.SignalInfo
	for _, si := range updatedMutableState.SignalInfos {
		sInfos = append(sInfos, si)
	}

	var srIDs []string
	for id := range updatedMutableState.SignalRequestedIDs {
		srIDs = append(srIDs, id)
	}
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:             updatedMutableState.ExecutionInfo,
			ExecutionStats:            updatedMutableState.ExecutionStats,
			ReplicationState:          updatedMutableState.ReplicationState,
			Condition:                 condition,
			UpsertActivityInfos:       aInfos,
			UpsertTimerInfos:          tInfos,
			UpsertChildExecutionInfos: cInfos,
			UpsertRequestCancelInfos:  rcInfos,
			UpsertSignalInfos:         sInfos,
			UpsertSignalRequestedIDs:  srIDs,
		},
		Encoding: pickRandomEncoding(),
	})
	return err
}

// ConflictResolveWorkflowExecution is  utility method to reset mutable state
func (s *TestBase) ConflictResolveWorkflowExecution(prevRunID string, prevLastWriteVersion int64, prevState int,
	info *p.WorkflowExecutionInfo, stats *p.ExecutionStats, replicationState *p.ReplicationState, nextEventID int64,
	activityInfos []*p.ActivityInfo, timerInfos []*p.TimerInfo, childExecutionInfos []*p.ChildExecutionInfo,
	requestCancelInfos []*p.RequestCancelInfo, signalInfos []*p.SignalInfo, ids []string) error {
	return s.ExecutionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		CurrentWorkflowCAS: &p.CurrentWorkflowCAS{
			PrevRunID:            prevRunID,
			PrevLastWriteVersion: prevLastWriteVersion,
			PrevState:            prevState,
		},
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:       info,
			ExecutionStats:      stats,
			ReplicationState:    replicationState,
			Condition:           nextEventID,
			ActivityInfos:       activityInfos,
			TimerInfos:          timerInfos,
			ChildExecutionInfos: childExecutionInfos,
			RequestCancelInfos:  requestCancelInfos,
			SignalInfos:         signalInfos,
			SignalRequestedIDs:  ids,
		},
		Encoding: pickRandomEncoding(),
	})
}

// ResetWorkflowExecution is  utility method to reset WF
func (s *TestBase) ResetWorkflowExecution(condition int64, info *p.WorkflowExecutionInfo,
	executionStats *p.ExecutionStats, replicationState *p.ReplicationState,
	activityInfos []*p.ActivityInfo, timerInfos []*p.TimerInfo, childExecutionInfos []*p.ChildExecutionInfo,
	requestCancelInfos []*p.RequestCancelInfo, signalInfos []*p.SignalInfo, ids []string, trasTasks, timerTasks, replTasks []p.Task,
	updateCurr bool, currInfo *p.WorkflowExecutionInfo, currExecutionStats *p.ExecutionStats, currReplicationState *p.ReplicationState,
	currTrasTasks, currTimerTasks []p.Task, forkRunID string, forkRunNextEventID int64) error {

	req := &p.ResetWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,

		BaseRunID:          forkRunID,
		BaseRunNextEventID: forkRunNextEventID,

		CurrentRunID:          currInfo.RunID,
		CurrentRunNextEventID: condition,

		CurrentWorkflowMutation: nil,

		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:    info,
			ExecutionStats:   executionStats,
			ReplicationState: replicationState,

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
		Encoding: pickRandomEncoding(),
	}

	if updateCurr {
		req.CurrentWorkflowMutation = &p.WorkflowMutation{
			ExecutionInfo:    currInfo,
			ExecutionStats:   currExecutionStats,
			ReplicationState: currReplicationState,

			TransferTasks: currTrasTasks,
			TimerTasks:    currTimerTasks,

			Condition: condition,
		}
	}

	return s.ExecutionManager.ResetWorkflowExecution(req)
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(info *p.WorkflowExecutionInfo) error {
	return s.ExecutionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
		DomainID:   info.DomainID,
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
	})
}

// DeleteCurrentWorkflowExecution is a utility method to delete the workflow current execution
func (s *TestBase) DeleteCurrentWorkflowExecution(info *p.WorkflowExecutionInfo) error {
	return s.ExecutionManager.DeleteCurrentWorkflowExecution(&p.DeleteCurrentWorkflowExecutionRequest{
		DomainID:   info.DomainID,
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
	})
}

// GetTransferTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTransferTasks(batchSize int, getAll bool) ([]*p.TransferTaskInfo, error) {
	result := []*p.TransferTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetTransferTasks(&p.GetTransferTasksRequest{
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
		atomic.StoreInt64(&s.ReadLevel, task.TaskID)
	}

	return result, nil
}

// GetReplicationTasks is a utility method to get tasks from replication task queue
func (s *TestBase) GetReplicationTasks(batchSize int, getAll bool) ([]*p.ReplicationTaskInfo, error) {
	result := []*p.ReplicationTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetReplicationTasks(&p.GetReplicationTasksRequest{
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
		atomic.StoreInt64(&s.ReplicationReadLevel, task.TaskID)
	}

	return result, nil
}

// CompleteTransferTask is a utility method to complete a transfer task
func (s *TestBase) CompleteTransferTask(taskID int64) error {

	return s.ExecutionManager.CompleteTransferTask(&p.CompleteTransferTaskRequest{
		TaskID: taskID,
	})
}

// RangeCompleteTransferTask is a utility method to complete a range of transfer tasks
func (s *TestBase) RangeCompleteTransferTask(exclusiveBeginTaskID int64, inclusiveEndTaskID int64) error {
	return s.ExecutionManager.RangeCompleteTransferTask(&p.RangeCompleteTransferTaskRequest{
		ExclusiveBeginTaskID: exclusiveBeginTaskID,
		InclusiveEndTaskID:   inclusiveEndTaskID,
	})
}

// CompleteReplicationTask is a utility method to complete a replication task
func (s *TestBase) CompleteReplicationTask(taskID int64) error {

	return s.ExecutionManager.CompleteReplicationTask(&p.CompleteReplicationTaskRequest{
		TaskID: taskID,
	})
}

// GetTimerIndexTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTimerIndexTasks(batchSize int, getAll bool) ([]*p.TimerTaskInfo, error) {
	result := []*p.TimerTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.ExecutionManager.GetTimerIndexTasks(&p.GetTimerIndexTasksRequest{
			MinTimestamp:  time.Time{},
			MaxTimestamp:  time.Unix(0, math.MaxInt64),
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
	return s.ExecutionManager.CompleteTimerTask(&p.CompleteTimerTaskRequest{
		VisibilityTimestamp: ts,
		TaskID:              taskID,
	})
}

// RangeCompleteTimerTask is a utility method to complete a range of timer tasks
func (s *TestBase) RangeCompleteTimerTask(inclusiveBeginTimestamp time.Time, exclusiveEndTimestamp time.Time) error {
	return s.ExecutionManager.RangeCompleteTimerTask(&p.RangeCompleteTimerTaskRequest{
		InclusiveBeginTimestamp: inclusiveBeginTimestamp,
		ExclusiveEndTimestamp:   exclusiveEndTimestamp,
	})
}

// CreateDecisionTask is a utility method to create a task
func (s *TestBase) CreateDecisionTask(domainID string, workflowExecution workflow.WorkflowExecution, taskList string,
	decisionScheduleID int64) (int64, error) {
	leaseResponse, err := s.TaskMgr.LeaseTaskList(&p.LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: p.TaskListTypeDecision,
	})
	if err != nil {
		return 0, err
	}

	taskID := s.GetNextSequenceNumber()
	tasks := []*p.CreateTaskInfo{
		{
			TaskID:    taskID,
			Execution: workflowExecution,
			Data: &p.TaskInfo{
				DomainID:   domainID,
				WorkflowID: *workflowExecution.WorkflowId,
				RunID:      *workflowExecution.RunId,
				TaskID:     taskID,
				ScheduleID: decisionScheduleID,
			},
		},
	}

	_, err = s.TaskMgr.CreateTasks(&p.CreateTasksRequest{
		TaskListInfo: leaseResponse.TaskListInfo,
		Tasks:        tasks,
	})

	if err != nil {
		return 0, err
	}

	return taskID, err
}

// CreateActivityTasks is a utility method to create tasks
func (s *TestBase) CreateActivityTasks(domainID string, workflowExecution workflow.WorkflowExecution,
	activities map[int64]string) ([]int64, error) {

	taskLists := make(map[string]*p.TaskListInfo)
	for _, tl := range activities {
		_, ok := taskLists[tl]
		if !ok {
			resp, err := s.TaskMgr.LeaseTaskList(
				&p.LeaseTaskListRequest{DomainID: domainID, TaskList: tl, TaskType: p.TaskListTypeActivity})
			if err != nil {
				return []int64{}, err
			}
			taskLists[tl] = resp.TaskListInfo
		}
	}

	var taskIDs []int64
	for activityScheduleID, taskList := range activities {
		taskID := s.GetNextSequenceNumber()
		tasks := []*p.CreateTaskInfo{
			{
				TaskID:    taskID,
				Execution: workflowExecution,
				Data: &p.TaskInfo{
					DomainID:               domainID,
					WorkflowID:             *workflowExecution.WorkflowId,
					RunID:                  *workflowExecution.RunId,
					TaskID:                 taskID,
					ScheduleID:             activityScheduleID,
					ScheduleToStartTimeout: defaultScheduleToStartTimeout,
				},
			},
		}
		_, err := s.TaskMgr.CreateTasks(&p.CreateTasksRequest{
			TaskListInfo: taskLists[taskList],
			Tasks:        tasks,
		})
		if err != nil {
			return nil, err
		}
		taskIDs = append(taskIDs, taskID)
	}

	return taskIDs, nil
}

// GetTasks is a utility method to get tasks from persistence
func (s *TestBase) GetTasks(domainID, taskList string, taskType int, batchSize int) (*p.GetTasksResponse, error) {
	response, err := s.TaskMgr.GetTasks(&p.GetTasksRequest{
		DomainID:     domainID,
		TaskList:     taskList,
		TaskType:     taskType,
		BatchSize:    batchSize,
		MaxReadLevel: common.Int64Ptr(math.MaxInt64),
	})

	if err != nil {
		return nil, err
	}

	return &p.GetTasksResponse{Tasks: response.Tasks}, nil
}

// CompleteTask is a utility method to complete a task
func (s *TestBase) CompleteTask(domainID, taskList string, taskType int, taskID int64, ackLevel int64) error {
	return s.TaskMgr.CompleteTask(&p.CompleteTaskRequest{
		TaskList: &p.TaskListInfo{
			DomainID: domainID,
			AckLevel: ackLevel,
			TaskType: taskType,
			Name:     taskList,
		},
		TaskID: taskID,
	})
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
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
	s.logger.Info("Clearing transfer tasks", tag.ShardRangeID(s.ShardInfo.RangeID), tag.ReadLevel(s.GetTransferReadLevel()))
	tasks, err := s.GetTransferTasks(100, true)
	if err != nil {
		s.logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.logger.Info("Deleting transfer task with ID", tag.TaskID(t.TaskID))
		s.CompleteTransferTask(t.TaskID)
		counter++
	}

	s.logger.Info("Deleted transfer tasks.", tag.Counter(counter))
	atomic.StoreInt64(&s.ReadLevel, 0)
}

// ClearReplicationQueue completes all tasks in replication queue
func (s *TestBase) ClearReplicationQueue() {
	s.logger.Info("Clearing replication tasks", tag.ShardRangeID(s.ShardInfo.RangeID), tag.ReadLevel(s.GetReplicationReadLevel()))
	tasks, err := s.GetReplicationTasks(100, true)
	if err != nil {
		s.logger.Fatal("Error during cleanup", tag.Error(err))
	}

	counter := 0
	for _, t := range tasks {
		s.logger.Info("Deleting replication task with ID", tag.TaskID(t.TaskID))
		s.CompleteReplicationTask(t.TaskID)
		counter++
	}

	s.logger.Info("Deleted replication tasks.", tag.Counter(counter))
	atomic.StoreInt64(&s.ReplicationReadLevel, 0)
}

// EqualTimesWithPrecision assertion that two times are equal within precision
func (s *TestBase) EqualTimesWithPrecision(t1, t2 time.Time, precision time.Duration) {
	s.True(timeComparator(t1, t2, precision),
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
	currentTime := time.Now()
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
func (s *TestBase) Publish(message interface{}) error {
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	return backoff.Retry(
		func() error {
			return s.DomainReplicationQueue.Publish(message)
		},
		retryPolicy,
		func(e error) bool {
			return common.IsPersistenceTransientError(e) || isMessageIDConflictError(e)
		})
}

func isMessageIDConflictError(err error) bool {
	_, ok := err.(*p.ConditionFailedError)
	return ok
}

// GetReplicationMessages is a utility method to get messages from the queue
func (s *TestBase) GetReplicationMessages(lastMessageID int, maxCount int) ([]*replicator.ReplicationTask, int, error) {
	return s.DomainReplicationQueue.GetReplicationMessages(lastMessageID, maxCount)
}

// UpdateAckLevel updates replication queue ack level
func (s *TestBase) UpdateAckLevel(lastProcessedMessageID int, clusterName string) error {
	return s.DomainReplicationQueue.UpdateAckLevel(lastProcessedMessageID, clusterName)
}

// GetAckLevels returns replication queue ack levels
func (s *TestBase) GetAckLevels() (map[string]int, error) {
	return s.DomainReplicationQueue.GetAckLevels()
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

// GenerateRandomDBName helper
func GenerateRandomDBName(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("workflow")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func pickRandomEncoding() common.EncodingType {
	// randomly pick json/thriftrw/empty as encoding type
	var encoding common.EncodingType
	i := rand.Intn(3)
	switch i {
	case 0:
		encoding = common.EncodingTypeJSON
	case 1:
		encoding = common.EncodingTypeThriftRW
	case 2:
		encoding = common.EncodingType("")
	}
	return encoding
}

func int64Ptr(i int64) *int64 {
	return &i
}

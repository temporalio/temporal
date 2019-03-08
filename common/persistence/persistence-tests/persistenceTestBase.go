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

	"github.com/stretchr/testify/suite"

	"fmt"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	pfactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/service/config"
)

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GetNextTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		DBName    string
		Cassandra struct {
			DBPort    int
			SchemaDir string
		}
		SQL struct {
			DBPort     int
			SchemaDir  string
			DriverName string
		}
		// TODO this is used for global domain test
		// when cross DC is public, remove EnableGlobalDomain
		EnableGlobalDomain bool // is global domain enabled
		IsMasterCluster    bool // is master cluster
		ClusterMetadata    cluster.Metadata
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		suite.Suite
		ShardMgr              p.ShardManager
		ExecutionMgrFactory   pfactory.Factory
		ExecutionManager      p.ExecutionManager
		TaskMgr               p.TaskManager
		HistoryMgr            p.HistoryManager
		HistoryV2Mgr          p.HistoryV2Manager
		MetadataManager       p.MetadataManager
		MetadataManagerV2     p.MetadataManager
		MetadataProxy         p.MetadataManager
		VisibilityMgr         p.VisibilityManager
		ShardInfo             *p.ShardInfo
		TaskIDGenerator       TransferTaskIDGenerator
		ClusterMetadata       cluster.Metadata
		ReadLevel             int64
		ReplicationReadLevel  int64
		DefaultTestCluster    PersistenceTestCluster
		VisibilityTestCluster PersistenceTestCluster
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

// NewTestBaseWithCassandra returns a persistence test base backed by cassandra datastore
func NewTestBaseWithCassandra(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + GenerateRandomDBName(10)
	}
	testCluster := cassandra.NewTestCluster(options.Cassandra.DBPort, options.DBName, options.Cassandra.SchemaDir)
	return newTestBase(options, testCluster)
}

// NewTestBaseWithSQL returns a new persistence test base backed by SQL
func NewTestBaseWithSQL(options *TestBaseOptions) TestBase {
	if options.DBName == "" {
		options.DBName = GenerateRandomDBName(10)
	}
	sqlOpts := options.SQL
	testCluster := sql.NewTestCluster(sqlOpts.DBPort, options.DBName, sqlOpts.SchemaDir, sqlOpts.DriverName)
	return newTestBase(options, testCluster)
}

func newTestBase(options *TestBaseOptions, testCluster PersistenceTestCluster) TestBase {
	metadata := options.ClusterMetadata
	if metadata == nil {
		metadata = cluster.GetTestClusterMetadata(options.EnableGlobalDomain, options.IsMasterCluster)
	}
	options.ClusterMetadata = metadata
	return TestBase{
		DefaultTestCluster:    testCluster,
		VisibilityTestCluster: testCluster,
		ClusterMetadata:       metadata,
	}
}

// Setup sets up the test base, must be called as part of SetupSuite
func (s *TestBase) Setup() {
	var err error
	shardID := 0
	clusterName := s.ClusterMetadata.GetCurrentClusterName()
	log := bark.NewLoggerFromLogrus(log.New())

	s.DefaultTestCluster.SetupTestDatabase()
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		s.VisibilityTestCluster.SetupTestDatabase()
	}

	cfg := s.DefaultTestCluster.Config()
	factory := pfactory.New(&cfg, clusterName, nil, log)

	s.TaskMgr, err = factory.NewTaskManager()
	s.fatalOnError("NewTaskManager", err)

	s.MetadataManager, err = factory.NewMetadataManager(pfactory.MetadataV1)
	s.fatalOnError("NewMetadataManager", err)

	s.MetadataManagerV2, err = factory.NewMetadataManager(pfactory.MetadataV2)
	s.fatalOnError("NewMetadataManager", err)

	s.MetadataProxy, err = factory.NewMetadataManager(pfactory.MetadataV1V2)
	s.fatalOnError("NewMetadataManager", err)

	s.HistoryMgr, err = factory.NewHistoryManager()
	s.fatalOnError("NewHistoryManager", err)

	s.HistoryV2Mgr, err = factory.NewHistoryV2Manager()
	// TODO SQL currently doesn't have support for HistoryV2Mgr.
	if err != nil {
		log.Warnf(fmt.Sprintf("failed to initialize historyV2, it is expected for MySQL, %v", err))
	}

	s.ShardMgr, err = factory.NewShardManager()
	s.fatalOnError("NewShardManager", err)

	s.ExecutionMgrFactory = factory
	s.ExecutionManager, err = factory.NewExecutionManager(shardID)
	s.fatalOnError("NewExecutionManager", err)

	visibilityFactory := factory
	if s.VisibilityTestCluster != s.DefaultTestCluster {
		vCfg := s.VisibilityTestCluster.Config()
		visibilityFactory = pfactory.New(&vCfg, clusterName, nil, log)
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
}

func (s *TestBase) fatalOnError(msg string, err error) {
	if err != nil {
		log.Fatalf("%v:%v", msg, err)
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

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(domainID string, workflowExecution workflow.WorkflowExecution, taskList,
	wType string, wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, timerTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            workflowExecution,
		TaskList:             taskList,
		WorkflowTypeName:     wType,
		WorkflowTimeout:      wTimeout,
		DecisionTimeoutValue: decisionTimeout,
		ExecutionContext:     executionContext,
		NextEventID:          nextEventID,
		LastProcessedEvent:   lastProcessedEventID,
		RangeID:              s.ShardInfo.RangeID,
		TransferTasks: []p.Task{
			&p.DecisionTask{
				TaskID:              s.GetNextSequenceNumber(),
				DomainID:            domainID,
				TaskList:            taskList,
				ScheduleID:          decisionScheduleID,
				VisibilityTimestamp: time.Now(),
			},
		},
		TimerTasks:                  timerTasks,
		DecisionScheduleID:          decisionScheduleID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
	})

	return response, err
}

// CreateWorkflowExecutionWithReplication is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionWithReplication(domainID string, workflowExecution workflow.WorkflowExecution,
	taskList, wType string, wTimeout int32, decisionTimeout int32, nextEventID int64,
	lastProcessedEventID int64, decisionScheduleID int64, state *p.ReplicationState, txTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	var transferTasks []p.Task
	var replicationTasks []p.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *p.DecisionTask, *p.ActivityTask, *p.CloseExecutionTask, *p.CancelExecutionTask, *p.StartChildExecutionTask, *p.SignalExecutionTask:
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
		RequestID:                   uuid.New(),
		DomainID:                    domainID,
		Execution:                   workflowExecution,
		TaskList:                    taskList,
		WorkflowTypeName:            wType,
		WorkflowTimeout:             wTimeout,
		DecisionTimeoutValue:        decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          lastProcessedEventID,
		RangeID:                     s.ShardInfo.RangeID,
		TransferTasks:               transferTasks,
		ReplicationTasks:            replicationTasks,
		DecisionScheduleID:          decisionScheduleID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
		ReplicationState:            state,
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
		RequestID:                   uuid.New(),
		DomainID:                    domainID,
		Execution:                   workflowExecution,
		TaskList:                    taskList,
		ExecutionContext:            executionContext,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          lastProcessedEventID,
		TransferTasks:               transferTasks,
		RangeID:                     s.ShardInfo.RangeID,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
	})

	return response, err
}

// CreateChildWorkflowExecution is a utility method to create child workflow executions
func (s *TestBase) CreateChildWorkflowExecution(domainID string, workflowExecution workflow.WorkflowExecution,
	parentDomainID string, parentExecution *workflow.WorkflowExecution, initiatedID int64, taskList, wType string,
	wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, timerTasks []p.Task) (*p.CreateWorkflowExecutionResponse, error) {
	response, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            workflowExecution,
		ParentDomainID:       parentDomainID,
		ParentExecution:      parentExecution,
		InitiatedID:          initiatedID,
		TaskList:             taskList,
		WorkflowTypeName:     wType,
		WorkflowTimeout:      wTimeout,
		DecisionTimeoutValue: decisionTimeout,
		ExecutionContext:     executionContext,
		NextEventID:          nextEventID,
		LastProcessedEvent:   lastProcessedEventID,
		RangeID:              s.ShardInfo.RangeID,
		TransferTasks: []p.Task{
			&p.DecisionTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: decisionScheduleID,
			},
		},
		TimerTasks:                  timerTasks,
		DecisionScheduleID:          decisionScheduleID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
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
func (s *TestBase) ContinueAsNewExecution(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	newExecution workflow.WorkflowExecution, nextEventID, decisionScheduleID int64) error {
	newdecisionTask := &p.DecisionTask{
		TaskID:     s.GetNextSequenceNumber(),
		DomainID:   updatedInfo.DomainID,
		TaskList:   updatedInfo.TaskList,
		ScheduleID: int64(decisionScheduleID),
	}

	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       updatedInfo,
		TransferTasks:       []p.Task{newdecisionTask},
		TimerTasks:          nil,
		Condition:           condition,
		DeleteTimerTask:     nil,
		RangeID:             s.ShardInfo.RangeID,
		UpsertActivityInfos: nil,
		DeleteActivityInfos: nil,
		UpserTimerInfos:     nil,
		DeleteTimerInfos:    nil,
		ContinueAsNew: &p.CreateWorkflowExecutionRequest{
			RequestID:                   uuid.New(),
			DomainID:                    updatedInfo.DomainID,
			Execution:                   newExecution,
			TaskList:                    updatedInfo.TaskList,
			WorkflowTypeName:            updatedInfo.WorkflowTypeName,
			WorkflowTimeout:             updatedInfo.WorkflowTimeout,
			DecisionTimeoutValue:        updatedInfo.DecisionTimeoutValue,
			ExecutionContext:            nil,
			NextEventID:                 nextEventID,
			LastProcessedEvent:          common.EmptyEventID,
			RangeID:                     s.ShardInfo.RangeID,
			TransferTasks:               nil,
			TimerTasks:                  nil,
			DecisionScheduleID:          decisionScheduleID,
			DecisionStartedID:           common.EmptyEventID,
			DecisionStartToCloseTimeout: 1,
			CreateWorkflowMode:          p.CreateWorkflowModeContinueAsNew,
			PreviousRunID:               updatedInfo.RunID,
		},
		Encoding: pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(updatedInfo *p.WorkflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, condition int64, timerTasks []p.Task, deleteTimerTask p.Task,
	upsertActivityInfos []*p.ActivityInfo, deleteActivityInfos []int64,
	upsertTimerInfos []*p.TimerInfo, deleteTimerInfos []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, decisionScheduleIDs, activityScheduleIDs,
		s.ShardInfo.RangeID, condition, timerTasks, deleteTimerTask, upsertActivityInfos, deleteActivityInfos,
		upsertTimerInfos, deleteTimerInfos, nil, nil, nil, nil,
		nil, nil, nil, "")
}

// UpdateWorkflowExecutionAndFinish is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionAndFinish(updatedInfo *p.WorkflowExecutionInfo, condition int64, retentionSecond int32) error {
	transferTasks := []p.Task{}
	transferTasks = append(transferTasks, &p.CloseExecutionTask{TaskID: s.GetNextSequenceNumber()})
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:        updatedInfo,
		TransferTasks:        transferTasks,
		TimerTasks:           nil,
		Condition:            condition,
		DeleteTimerTask:      nil,
		RangeID:              s.ShardInfo.RangeID,
		UpsertActivityInfos:  nil,
		DeleteActivityInfos:  nil,
		UpserTimerInfos:      nil,
		DeleteTimerInfos:     nil,
		FinishedExecutionTTL: retentionSecond,
		FinishExecution:      true,
		Encoding:             pickRandomEncoding(),
	})
	return err
}

// UpsertChildExecutionsState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertChildExecutionsState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	upsertChildInfos []*p.ChildExecutionInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, upsertChildInfos, nil, nil, nil,
		nil, nil, nil, "")
}

// UpsertRequestCancelState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertRequestCancelState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	upsertCancelInfos []*p.RequestCancelInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, upsertCancelInfos, nil,
		nil, nil, nil, "")
}

// UpsertSignalInfoState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalInfoState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	upsertSignalInfos []*p.SignalInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		upsertSignalInfos, nil, nil, "")
}

// UpsertSignalsRequestedState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalsRequestedState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	upsertSignalsRequested []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, upsertSignalsRequested, "")
}

// DeleteChildExecutionsState is a utility method to delete child execution from mutable state
func (s *TestBase) DeleteChildExecutionsState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	deleteChildInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, &deleteChildInfo, nil, nil,
		nil, nil, nil, "")
}

// DeleteCancelState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteCancelState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	deleteCancelInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, &deleteCancelInfo,
		nil, nil, nil, "")
}

// DeleteSignalState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteSignalState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	deleteSignalInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, &deleteSignalInfo, nil, "")
}

// DeleteSignalsRequestedState is a utility method to delete mutable state of workflow execution
func (s *TestBase) DeleteSignalsRequestedState(updatedInfo *p.WorkflowExecutionInfo, condition int64,
	deleteSignalsRequestedID string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, deleteSignalsRequestedID)
}

// UpdateWorklowStateAndReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorklowStateAndReplication(updatedInfo *p.WorkflowExecutionInfo,
	updatedReplicationState *p.ReplicationState, newBufferedReplicationTask *p.BufferedReplicationTask,
	deleteBufferedReplicationTask *int64, condition int64, txTasks []p.Task) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedReplicationState, nil, nil,
		s.ShardInfo.RangeID, condition, nil, txTasks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "",
		newBufferedReplicationTask, deleteBufferedReplicationTask)
}

// UpdateWorkflowExecutionWithRangeID is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithRangeID(updatedInfo *p.WorkflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, rangeID, condition int64, timerTasks []p.Task, deleteTimerTask p.Task,
	upsertActivityInfos []*p.ActivityInfo, deleteActivityInfos []int64, upsertTimerInfos []*p.TimerInfo,
	deleteTimerInfos []string, upsertChildInfos []*p.ChildExecutionInfo, deleteChildInfo *int64,
	upsertCancelInfos []*p.RequestCancelInfo, deleteCancelInfo *int64,
	upsertSignalInfos []*p.SignalInfo, deleteSignalInfo *int64,
	upsertSignalRequestedIDs []string, deleteSignalRequestedID string) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, nil, decisionScheduleIDs, activityScheduleIDs, rangeID,
		condition, timerTasks, []p.Task{}, deleteTimerTask, upsertActivityInfos, deleteActivityInfos, upsertTimerInfos, deleteTimerInfos,
		upsertChildInfos, deleteChildInfo, upsertCancelInfos, deleteCancelInfo, upsertSignalInfos, deleteSignalInfo,
		upsertSignalRequestedIDs, deleteSignalRequestedID, nil, nil)
}

// UpdateWorkflowExecutionWithReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithReplication(updatedInfo *p.WorkflowExecutionInfo,
	updatedReplicationState *p.ReplicationState, decisionScheduleIDs []int64, activityScheduleIDs []int64, rangeID,
	condition int64, timerTasks []p.Task, txTasks []p.Task, deleteTimerTask p.Task, upsertActivityInfos []*p.ActivityInfo,
	deleteActivityInfos []int64, upsertTimerInfos []*p.TimerInfo, deleteTimerInfos []string,
	upsertChildInfos []*p.ChildExecutionInfo, deleteChildInfo *int64, upsertCancelInfos []*p.RequestCancelInfo,
	deleteCancelInfo *int64, upsertSignalInfos []*p.SignalInfo, deleteSignalInfo *int64, upsertSignalRequestedIDs []string,
	deleteSignalRequestedID string, newBufferedReplicationTask *p.BufferedReplicationTask,
	deleteBufferedReplicationTask *int64) error {
	var transferTasks []p.Task
	var replicationTasks []p.Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *p.DecisionTask, *p.ActivityTask, *p.CloseExecutionTask, *p.CancelExecutionTask, *p.StartChildExecutionTask, *p.SignalExecutionTask:
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
		ExecutionInfo:                 updatedInfo,
		ReplicationState:              updatedReplicationState,
		TransferTasks:                 transferTasks,
		ReplicationTasks:              replicationTasks,
		TimerTasks:                    timerTasks,
		Condition:                     condition,
		DeleteTimerTask:               deleteTimerTask,
		RangeID:                       rangeID,
		UpsertActivityInfos:           upsertActivityInfos,
		DeleteActivityInfos:           deleteActivityInfos,
		UpserTimerInfos:               upsertTimerInfos,
		DeleteTimerInfos:              deleteTimerInfos,
		UpsertChildExecutionInfos:     upsertChildInfos,
		DeleteChildExecutionInfo:      deleteChildInfo,
		UpsertRequestCancelInfos:      upsertCancelInfos,
		DeleteRequestCancelInfo:       deleteCancelInfo,
		UpsertSignalInfos:             upsertSignalInfos,
		DeleteSignalInfo:              deleteSignalInfo,
		UpsertSignalRequestedIDs:      upsertSignalRequestedIDs,
		DeleteSignalRequestedID:       deleteSignalRequestedID,
		NewBufferedReplicationTask:    newBufferedReplicationTask,
		DeleteBufferedReplicationTask: deleteBufferedReplicationTask,
		Encoding:                      pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionWithTransferTasks is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithTransferTasks(
	updatedInfo *p.WorkflowExecutionInfo, condition int64, transferTasks []p.Task, upsertActivityInfo []*p.ActivityInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       updatedInfo,
		TransferTasks:       transferTasks,
		Condition:           condition,
		UpsertActivityInfos: upsertActivityInfo,
		RangeID:             s.ShardInfo.RangeID,
		Encoding:            pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForChildExecutionsInitiated is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForChildExecutionsInitiated(
	updatedInfo *p.WorkflowExecutionInfo, condition int64, transferTasks []p.Task, childInfos []*p.ChildExecutionInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:             updatedInfo,
		TransferTasks:             transferTasks,
		Condition:                 condition,
		UpsertChildExecutionInfos: childInfos,
		RangeID:                   s.ShardInfo.RangeID,
		Encoding:                  pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForRequestCancel is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForRequestCancel(
	updatedInfo *p.WorkflowExecutionInfo, condition int64, transferTasks []p.Task,
	upsertRequestCancelInfo []*p.RequestCancelInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:            updatedInfo,
		TransferTasks:            transferTasks,
		Condition:                condition,
		UpsertRequestCancelInfos: upsertRequestCancelInfo,
		RangeID:                  s.ShardInfo.RangeID,
		Encoding:                 pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForSignal is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForSignal(
	updatedInfo *p.WorkflowExecutionInfo, condition int64, transferTasks []p.Task,
	upsertSignalInfos []*p.SignalInfo) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:     updatedInfo,
		TransferTasks:     transferTasks,
		Condition:         condition,
		UpsertSignalInfos: upsertSignalInfos,
		RangeID:           s.ShardInfo.RangeID,
		Encoding:          pickRandomEncoding(),
	})
	return err
}

// UpdateWorkflowExecutionForBufferEvents is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForBufferEvents(
	updatedInfo *p.WorkflowExecutionInfo, rState *p.ReplicationState, condition int64,
	bufferEvents []*workflow.HistoryEvent, clearBufferedEvents bool) error {
	_, err := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       updatedInfo,
		ReplicationState:    rState,
		NewBufferedEvents:   bufferEvents,
		Condition:           condition,
		ClearBufferedEvents: clearBufferedEvents,

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
		ExecutionInfo:             updatedMutableState.ExecutionInfo,
		ReplicationState:          updatedMutableState.ReplicationState,
		Condition:                 condition,
		RangeID:                   s.ShardInfo.RangeID,
		UpsertActivityInfos:       aInfos,
		UpserTimerInfos:           tInfos,
		UpsertChildExecutionInfos: cInfos,
		UpsertRequestCancelInfos:  rcInfos,
		UpsertSignalInfos:         sInfos,
		UpsertSignalRequestedIDs:  srIDs,
		Encoding:                  pickRandomEncoding(),
	})
	return err
}

// ResetMutableState is  utility method to reset mutable state
func (s *TestBase) ResetMutableState(prevRunID string, info *p.WorkflowExecutionInfo, replicationState *p.ReplicationState, nextEventID int64,
	activityInfos []*p.ActivityInfo, timerInfos []*p.TimerInfo, childExecutionInfos []*p.ChildExecutionInfo,
	requestCancelInfos []*p.RequestCancelInfo, signalInfos []*p.SignalInfo, ids []string) error {
	return s.ExecutionManager.ResetMutableState(&p.ResetMutableStateRequest{
		PrevRunID:                 prevRunID,
		ExecutionInfo:             info,
		ReplicationState:          replicationState,
		Condition:                 nextEventID,
		RangeID:                   s.ShardInfo.RangeID,
		InsertActivityInfos:       activityInfos,
		InsertTimerInfos:          timerInfos,
		InsertChildExecutionInfos: childExecutionInfos,
		InsertRequestCancelInfos:  requestCancelInfos,
		InsertSignalInfos:         signalInfos,
		InsertSignalRequestedIDs:  ids,
		Encoding:                  pickRandomEncoding(),
	})
}

// ResetWorkflowExecution is  utility method to reset WF
func (s *TestBase) ResetWorkflowExecution(condition int64, info *p.WorkflowExecutionInfo, replicationState *p.ReplicationState,
	activityInfos []*p.ActivityInfo, timerInfos []*p.TimerInfo, childExecutionInfos []*p.ChildExecutionInfo,
	requestCancelInfos []*p.RequestCancelInfo, signalInfos []*p.SignalInfo, ids []string, trasTasks, timerTasks, replTasks []p.Task,
	updateCurr bool, currInfo *p.WorkflowExecutionInfo, currReplicationState *p.ReplicationState,
	currTrasTasks, currTimerTasks []p.Task, forkRunID string, forkRunNextEventID int64, prevRunVersion int64) error {

	prevRunState := p.WorkflowStateCompleted
	if updateCurr {
		prevRunState = p.WorkflowStateRunning
	}

	return s.ExecutionManager.ResetWorkflowExecution(&p.ResetWorkflowExecutionRequest{
		BaseRunID:          forkRunID,
		BaseRunNextEventID: forkRunNextEventID,

		PrevRunVersion: prevRunVersion,
		PrevRunState:   prevRunState,

		Condition: condition,
		RangeID:   s.ShardInfo.RangeID,

		UpdateCurr:           updateCurr,
		CurrExecutionInfo:    currInfo,
		CurrReplicationState: currReplicationState,
		CurrTransferTasks:    currTrasTasks,
		CurrTimerTasks:       currTimerTasks,

		InsertExecutionInfo:       info,
		InsertReplicationState:    replicationState,
		InsertTransferTasks:       trasTasks,
		InsertTimerTasks:          timerTasks,
		InsertReplicationTasks:    replTasks,
		InsertActivityInfos:       activityInfos,
		InsertTimerInfos:          timerInfos,
		InsertChildExecutionInfos: childExecutionInfos,
		InsertRequestCancelInfos:  requestCancelInfos,
		InsertSignalInfos:         signalInfos,
		InsertSignalRequestedIDs:  ids,
		Encoding:                  pickRandomEncoding(),
	})
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(info *p.WorkflowExecutionInfo) error {
	return s.ExecutionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
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
					DomainID:   domainID,
					WorkflowID: *workflowExecution.WorkflowId,
					RunID:      *workflowExecution.RunId,
					TaskID:     taskID,
					ScheduleID: activityScheduleID,
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
		MaxReadLevel: math.MaxInt64,
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
	taskID, _ := s.TaskIDGenerator.GetNextTransferTaskID()
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
	log.Infof("Clearing transfer tasks (RangeID: %v, ReadLevel: %v)",
		s.ShardInfo.RangeID, s.GetTransferReadLevel())
	tasks, err := s.GetTransferTasks(100, true)
	if err != nil {
		log.Fatalf("Error during cleanup: %v", err)
	}

	counter := 0
	for _, t := range tasks {
		log.Infof("Deleting transfer task with ID: %v", t.TaskID)
		s.CompleteTransferTask(t.TaskID)
		counter++
	}

	log.Infof("Deleted '%v' transfer tasks.", counter)
	atomic.StoreInt64(&s.ReadLevel, 0)
}

// ClearReplicationQueue completes all tasks in replication queue
func (s *TestBase) ClearReplicationQueue() {
	log.Infof("Clearing replication tasks (RangeID: %v, ReadLevel: %v)",
		s.ShardInfo.RangeID, s.GetReplicationReadLevel())
	tasks, err := s.GetReplicationTasks(100, true)
	if err != nil {
		log.Fatalf("Error during cleanup: %v", err)
	}

	counter := 0
	for _, t := range tasks {
		log.Infof("Deleting replication task with ID: %v", t.TaskID)
		s.CompleteReplicationTask(t.TaskID)
		counter++
	}

	log.Infof("Deleted '%v' replication tasks.", counter)
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

func validateTimeRange(t time.Time, expectedDuration time.Duration) bool {
	currentTime := time.Now()
	diff := time.Duration(currentTime.UnixNano() - t.UnixNano())
	if diff > expectedDuration {
		log.Infof("Current time: %v, Application time: %v, Differenrce: %v", currentTime, t, diff)
		return false
	}
	return true
}

// GetNextTransferTaskID helper
func (g *TestTransferTaskIDGenerator) GetNextTransferTaskID() (int64, error) {
	return atomic.AddInt64(&g.seqNum, 1), nil
}

// GetTransferTaskIDs helper
func (g *TestTransferTaskIDGenerator) GetTransferTaskIDs(number int) ([]int64, error) {
	result := []int64{}
	for i := 0; i < number; i++ {
		id, err := g.GetNextTransferTaskID()
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

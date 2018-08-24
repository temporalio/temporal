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

package persistence

import (
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
	testPort                 = 0
	testUser                 = ""
	testPassword             = ""
	testDatacenter           = ""
	testSchemaDir            = "../.."
)

type (
	// TransferTaskIDGenerator generates IDs for transfer tasks written by helper methods
	TransferTaskIDGenerator interface {
		GetNextTransferTaskID() (int64, error)
	}

	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		ClusterHost     string
		ClusterPort     int
		ClusterUser     string
		ClusterPassword string
		KeySpace        string
		Datacenter      string
		DropKeySpace    bool
		SchemaDir       string
		// TODO this is used for global domain test
		// when crtoss DC is public, remove EnableGlobalDomain
		EnableGlobalDomain bool
		IsMasterCluster    bool
	}

	// TestBase wraps the base setup needed to create workflows over persistence layer.
	TestBase struct {
		ShardMgr             ShardManager
		ExecutionMgrFactory  ExecutionManagerFactory
		WorkflowMgr          ExecutionManager
		TaskMgr              TaskManager
		HistoryMgr           HistoryManager
		MetadataManager      MetadataManager
		MetadataManagerV2    MetadataManager
		MetadataProxy        MetadataManager
		VisibilityMgr        VisibilityManager
		ShardInfo            *ShardInfo
		TaskIDGenerator      TransferTaskIDGenerator
		ClusterMetadata      cluster.Metadata
		readLevel            int64
		replicationReadLevel int64
		CassandraTestCluster
	}

	// CassandraTestCluster allows executing cassandra operations in testing.
	CassandraTestCluster struct {
		port     int
		keyspace string
		cluster  *gocql.ClusterConfig
		session  *gocql.Session
	}

	testExecutionMgrFactory struct {
		options   TestBaseOptions
		cassandra CassandraTestCluster
		logger    bark.Logger
	}

	testTransferTaskIDGenerator struct {
		seqNum int64
	}
)

func (g *testTransferTaskIDGenerator) GetNextTransferTaskID() (int64, error) {
	return atomic.AddInt64(&g.seqNum, 1), nil
}

// SetupWorkflowStoreWithOptions to setup workflow test base
func (s *TestBase) SetupWorkflowStoreWithOptions(options TestBaseOptions, metadata cluster.Metadata) {
	log := bark.NewLoggerFromLogrus(log.New())

	if metadata == nil {
		s.ClusterMetadata = cluster.GetTestClusterMetadata(
			options.EnableGlobalDomain,
			options.IsMasterCluster,
		)
	} else {
		s.ClusterMetadata = metadata
		log = log.WithField("Cluster", metadata.GetCurrentClusterName())
	}
	currentClusterName := s.ClusterMetadata.GetCurrentClusterName()

	// Setup Workflow keyspace and deploy schema for tests
	s.CassandraTestCluster.setupTestCluster(options)
	shardID := 0
	var err error
	s.ShardMgr, err = NewCassandraShardPersistence(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}
	s.ExecutionMgrFactory, err = NewCassandraPersistenceClientFactory(options.ClusterHost, options.ClusterPort,
		options.ClusterUser, options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, 2, log, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	// Create an ExecutionManager for the shard for use in unit tests
	s.WorkflowMgr, err = s.ExecutionMgrFactory.CreateExecutionManager(shardID)
	if err != nil {
		log.Fatal(err)
	}
	s.TaskMgr, err = NewCassandraTaskPersistence(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace,
		log)
	if err != nil {
		log.Fatal(err)
	}

	s.HistoryMgr, err = NewCassandraHistoryPersistence(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, 2, log)
	if err != nil {
		log.Fatal(err)
	}

	s.MetadataManager, err = NewCassandraMetadataPersistence(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}

	s.MetadataManagerV2, err = NewCassandraMetadataPersistenceV2(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}

	s.MetadataProxy, err = NewMetadataManagerProxy(options.ClusterHost, options.ClusterPort, options.ClusterUser,
		options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, currentClusterName, log)
	if err != nil {
		log.Fatal(err)
	}

	s.VisibilityMgr, err = NewCassandraVisibilityPersistence(options.ClusterHost, options.ClusterPort,
		options.ClusterUser, options.ClusterPassword, options.Datacenter, s.CassandraTestCluster.keyspace, log)
	if err != nil {
		log.Fatal(err)
	}

	s.TaskIDGenerator = &testTransferTaskIDGenerator{}

	// Create a shard for test
	s.readLevel = 0
	s.replicationReadLevel = 0
	s.ShardInfo = &ShardInfo{
		ShardID:                 shardID,
		RangeID:                 0,
		TransferAckLevel:        0,
		ReplicationAckLevel:     0,
		TimerAckLevel:           time.Time{},
		ClusterTimerAckLevel:    map[string]time.Time{currentClusterName: time.Time{}},
		ClusterTransferAckLevel: map[string]int64{currentClusterName: 0},
	}

	err1 := s.ShardMgr.CreateShard(&CreateShardRequest{
		ShardInfo: s.ShardInfo,
	})
	if err1 != nil {
		log.Fatal(err1)
	}
}

// CreateShard is a utility method to create the shard using persistence layer
func (s *TestBase) CreateShard(shardID int, owner string, rangeID int64) error {
	info := &ShardInfo{
		ShardID: shardID,
		Owner:   owner,
		RangeID: rangeID,
	}

	return s.ShardMgr.CreateShard(&CreateShardRequest{
		ShardInfo: info,
	})
}

// GetShard is a utility method to get the shard using persistence layer
func (s *TestBase) GetShard(shardID int) (*ShardInfo, error) {
	response, err := s.ShardMgr.GetShard(&GetShardRequest{
		ShardID: shardID,
	})

	if err != nil {
		return nil, err
	}

	return response.ShardInfo, nil
}

// UpdateShard is a utility method to update the shard using persistence layer
func (s *TestBase) UpdateShard(updatedInfo *ShardInfo, previousRangeID int64) error {
	return s.ShardMgr.UpdateShard(&UpdateShardRequest{
		ShardInfo:       updatedInfo,
		PreviousRangeID: previousRangeID,
	})
}

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(domainID string, workflowExecution workflow.WorkflowExecution, taskList,
	wType string, wTimeout int32, decisionTimeout int32, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleID int64, timerTasks []Task) (*CreateWorkflowExecutionResponse, error) {
	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
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
		TransferTasks: []Task{
			&DecisionTask{
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

// CreateWorkflowExecutionWithReplication is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionWithReplication(domainID string, workflowExecution workflow.WorkflowExecution,
	taskList, wType string, wTimeout int32, decisionTimeout int32, nextEventID int64,
	lastProcessedEventID int64, decisionScheduleID int64, state *ReplicationState, txTasks []Task) (*CreateWorkflowExecutionResponse, error) {
	var transferTasks []Task
	var replicationTasks []Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *DecisionTask, *ActivityTask, *CloseExecutionTask, *CancelExecutionTask, *StartChildExecutionTask, *SignalExecutionTask:
			transferTasks = append(transferTasks, t)
		case *HistoryReplicationTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}

	transferTasks = append(transferTasks, &DecisionTask{
		TaskID:     s.GetNextSequenceNumber(),
		DomainID:   domainID,
		TaskList:   taskList,
		ScheduleID: decisionScheduleID,
	})
	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
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
	decisionScheduleIDs []int64, activityScheduleIDs []int64) (*CreateWorkflowExecutionResponse, error) {

	transferTasks := []Task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks,
			&DecisionTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: int64(decisionScheduleID),
			})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks,
			&ActivityTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: int64(activityScheduleID),
			})
	}

	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
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
	decisionScheduleID int64, timerTasks []Task) (*CreateWorkflowExecutionResponse, error) {
	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
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
		TransferTasks: []Task{
			&DecisionTask{
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

// GetWorkflowExecutionInfo is a utility method to retrieve execution info
func (s *TestBase) GetWorkflowExecutionInfo(domainID string, workflowExecution workflow.WorkflowExecution) (
	*WorkflowMutableState, error) {
	response, err := s.WorkflowMgr.GetWorkflowExecution(&GetWorkflowExecutionRequest{
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
	response, err := s.WorkflowMgr.GetCurrentExecution(&GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})

	if err != nil {
		return "", err
	}

	return response.RunID, nil
}

// ContinueAsNewExecution is a utility method to create workflow executions
func (s *TestBase) ContinueAsNewExecution(updatedInfo *WorkflowExecutionInfo, condition int64,
	newExecution workflow.WorkflowExecution, nextEventID, decisionScheduleID int64) error {
	newdecisionTask := &DecisionTask{
		TaskID:     s.GetNextSequenceNumber(),
		DomainID:   updatedInfo.DomainID,
		TaskList:   updatedInfo.TaskList,
		ScheduleID: int64(decisionScheduleID),
	}

	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:       updatedInfo,
		TransferTasks:       []Task{newdecisionTask},
		TimerTasks:          nil,
		Condition:           condition,
		DeleteTimerTask:     nil,
		RangeID:             s.ShardInfo.RangeID,
		UpsertActivityInfos: nil,
		DeleteActivityInfos: nil,
		UpserTimerInfos:     nil,
		DeleteTimerInfos:    nil,
		ContinueAsNew: &CreateWorkflowExecutionRequest{
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
			ContinueAsNew:               true,
			PreviousRunID:               updatedInfo.RunID,
		},
	})
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(updatedInfo *WorkflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, condition int64, timerTasks []Task, deleteTimerTask Task,
	upsertActivityInfos []*ActivityInfo, deleteActivityInfos []int64,
	upsertTimerInfos []*TimerInfo, deleteTimerInfos []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, decisionScheduleIDs, activityScheduleIDs,
		s.ShardInfo.RangeID, condition, timerTasks, deleteTimerTask, upsertActivityInfos, deleteActivityInfos,
		upsertTimerInfos, deleteTimerInfos, nil, nil, nil, nil,
		nil, nil, nil, "")
}

// UpdateWorkflowExecutionAndFinish is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionAndFinish(updatedInfo *WorkflowExecutionInfo, condition int64, retentionSecond int32) error {
	transferTasks := []Task{}
	transferTasks = append(transferTasks, &CloseExecutionTask{TaskID: s.GetNextSequenceNumber()})
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
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
	})
}

// UpsertChildExecutionsState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertChildExecutionsState(updatedInfo *WorkflowExecutionInfo, condition int64,
	upsertChildInfos []*ChildExecutionInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, upsertChildInfos, nil, nil, nil,
		nil, nil, nil, "")
}

// UpsertRequestCancelState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertRequestCancelState(updatedInfo *WorkflowExecutionInfo, condition int64,
	upsertCancelInfos []*RequestCancelInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, upsertCancelInfos, nil,
		nil, nil, nil, "")
}

// UpsertSignalInfoState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalInfoState(updatedInfo *WorkflowExecutionInfo, condition int64,
	upsertSignalInfos []*SignalInfo) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		upsertSignalInfos, nil, nil, "")
}

// UpsertSignalsRequestedState is a utility method to update mutable state of workflow execution
func (s *TestBase) UpsertSignalsRequestedState(updatedInfo *WorkflowExecutionInfo, condition int64,
	upsertSignalsRequested []string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, upsertSignalsRequested, "")
}

// DeleteChildExecutionsState is a utility method to delete child execution from mutable state
func (s *TestBase) DeleteChildExecutionsState(updatedInfo *WorkflowExecutionInfo, condition int64,
	deleteChildInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, &deleteChildInfo, nil, nil,
		nil, nil, nil, "")
}

// DeleteCancelState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteCancelState(updatedInfo *WorkflowExecutionInfo, condition int64,
	deleteCancelInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, &deleteCancelInfo,
		nil, nil, nil, "")
}

// DeleteSignalState is a utility method to delete request cancel state from mutable state
func (s *TestBase) DeleteSignalState(updatedInfo *WorkflowExecutionInfo, condition int64,
	deleteSignalInfo int64) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, &deleteSignalInfo, nil, "")
}

// DeleteSignalsRequestedState is a utility method to delete mutable state of workflow execution
func (s *TestBase) DeleteSignalsRequestedState(updatedInfo *WorkflowExecutionInfo, condition int64,
	deleteSignalsRequestedID string) error {
	return s.UpdateWorkflowExecutionWithRangeID(updatedInfo, nil, nil,
		s.ShardInfo.RangeID, condition, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, deleteSignalsRequestedID)
}

// UpdateWorklowStateAndReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorklowStateAndReplication(updatedInfo *WorkflowExecutionInfo,
	updatedReplicationState *ReplicationState, newBufferedReplicationTask *BufferedReplicationTask,
	deleteBufferedReplicationTask *int64, condition int64, txTasks []Task) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, updatedReplicationState, nil, nil,
		s.ShardInfo.RangeID, condition, nil, txTasks, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "",
		newBufferedReplicationTask, deleteBufferedReplicationTask)
}

// UpdateWorkflowExecutionWithRangeID is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithRangeID(updatedInfo *WorkflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, rangeID, condition int64, timerTasks []Task, deleteTimerTask Task,
	upsertActivityInfos []*ActivityInfo, deleteActivityInfos []int64, upsertTimerInfos []*TimerInfo,
	deleteTimerInfos []string, upsertChildInfos []*ChildExecutionInfo, deleteChildInfo *int64,
	upsertCancelInfos []*RequestCancelInfo, deleteCancelInfo *int64,
	upsertSignalInfos []*SignalInfo, deleteSignalInfo *int64,
	upsertSignalRequestedIDs []string, deleteSignalRequestedID string) error {
	return s.UpdateWorkflowExecutionWithReplication(updatedInfo, nil, decisionScheduleIDs, activityScheduleIDs, rangeID,
		condition, timerTasks, []Task{}, deleteTimerTask, upsertActivityInfos, deleteActivityInfos, upsertTimerInfos, deleteTimerInfos,
		upsertChildInfos, deleteChildInfo, upsertCancelInfos, deleteCancelInfo, upsertSignalInfos, deleteSignalInfo,
		upsertSignalRequestedIDs, deleteSignalRequestedID, nil, nil)
}

// UpdateWorkflowExecutionWithReplication is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithReplication(updatedInfo *WorkflowExecutionInfo,
	updatedReplicationState *ReplicationState, decisionScheduleIDs []int64, activityScheduleIDs []int64, rangeID,
	condition int64, timerTasks []Task, txTasks []Task, deleteTimerTask Task, upsertActivityInfos []*ActivityInfo,
	deleteActivityInfos []int64, upsertTimerInfos []*TimerInfo, deleteTimerInfos []string,
	upsertChildInfos []*ChildExecutionInfo, deleteChildInfo *int64, upsertCancelInfos []*RequestCancelInfo,
	deleteCancelInfo *int64, upsertSignalInfos []*SignalInfo, deleteSignalInfo *int64, upsertSignalRequestedIDs []string,
	deleteSignalRequestedID string, newBufferedReplicationTask *BufferedReplicationTask,
	deleteBufferedReplicationTask *int64) error {
	var transferTasks []Task
	var replicationTasks []Task
	for _, task := range txTasks {
		switch t := task.(type) {
		case *DecisionTask, *ActivityTask, *CloseExecutionTask, *CancelExecutionTask, *StartChildExecutionTask, *SignalExecutionTask:
			transferTasks = append(transferTasks, t)
		case *HistoryReplicationTask:
			replicationTasks = append(replicationTasks, t)
		default:
			panic("Unknown transfer task type.")
		}
	}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &DecisionTask{
			TaskID:     s.GetNextSequenceNumber(),
			DomainID:   updatedInfo.DomainID,
			TaskList:   updatedInfo.TaskList,
			ScheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &ActivityTask{
			TaskID:     s.GetNextSequenceNumber(),
			DomainID:   updatedInfo.DomainID,
			TaskList:   updatedInfo.TaskList,
			ScheduleID: int64(activityScheduleID)})
	}

	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
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
	})
}

// UpdateWorkflowExecutionWithTransferTasks is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionWithTransferTasks(
	updatedInfo *WorkflowExecutionInfo, condition int64, transferTasks []Task, upsertActivityInfo []*ActivityInfo) error {
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:       updatedInfo,
		TransferTasks:       transferTasks,
		Condition:           condition,
		UpsertActivityInfos: upsertActivityInfo,
		RangeID:             s.ShardInfo.RangeID,
	})
}

// UpdateWorkflowExecutionForChildExecutionsInitiated is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForChildExecutionsInitiated(
	updatedInfo *WorkflowExecutionInfo, condition int64, transferTasks []Task, childInfos []*ChildExecutionInfo) error {
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:             updatedInfo,
		TransferTasks:             transferTasks,
		Condition:                 condition,
		UpsertChildExecutionInfos: childInfos,
		RangeID:                   s.ShardInfo.RangeID,
	})
}

// UpdateWorkflowExecutionForRequestCancel is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForRequestCancel(
	updatedInfo *WorkflowExecutionInfo, condition int64, transferTasks []Task,
	upsertRequestCancelInfo []*RequestCancelInfo) error {
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:            updatedInfo,
		TransferTasks:            transferTasks,
		Condition:                condition,
		UpsertRequestCancelInfos: upsertRequestCancelInfo,
		RangeID:                  s.ShardInfo.RangeID,
	})
}

// UpdateWorkflowExecutionForSignal is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForSignal(
	updatedInfo *WorkflowExecutionInfo, condition int64, transferTasks []Task,
	upsertSignalInfos []*SignalInfo) error {
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:     updatedInfo,
		TransferTasks:     transferTasks,
		Condition:         condition,
		UpsertSignalInfos: upsertSignalInfos,
		RangeID:           s.ShardInfo.RangeID,
	})
}

// UpdateWorkflowExecutionForBufferEvents is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecutionForBufferEvents(
	updatedInfo *WorkflowExecutionInfo, rState *ReplicationState, condition int64,
	bufferEvents *SerializedHistoryEventBatch) error {
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:     updatedInfo,
		ReplicationState:  rState,
		NewBufferedEvents: bufferEvents,
		Condition:         condition,
		RangeID:           s.ShardInfo.RangeID,
	})
}

// UpdateAllMutableState is a utility method to update workflow execution
func (s *TestBase) UpdateAllMutableState(updatedMutableState *WorkflowMutableState, condition int64) error {
	var aInfos []*ActivityInfo
	for _, ai := range updatedMutableState.ActivitInfos {
		aInfos = append(aInfos, ai)
	}

	var tInfos []*TimerInfo
	for _, ti := range updatedMutableState.TimerInfos {
		tInfos = append(tInfos, ti)
	}

	var cInfos []*ChildExecutionInfo
	for _, ci := range updatedMutableState.ChildExecutionInfos {
		cInfos = append(cInfos, ci)
	}

	var rcInfos []*RequestCancelInfo
	for _, rci := range updatedMutableState.RequestCancelInfos {
		rcInfos = append(rcInfos, rci)
	}

	var sInfos []*SignalInfo
	for _, si := range updatedMutableState.SignalInfos {
		sInfos = append(sInfos, si)
	}

	var srIDs []string
	for id := range updatedMutableState.SignalRequestedIDs {
		srIDs = append(srIDs, id)
	}
	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
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
	})
}

// ResetMutableState is  utility method to reset mutable state
func (s *TestBase) ResetMutableState(prevRunID string, info *WorkflowExecutionInfo, replicationState *ReplicationState, nextEventID int64,
	activityInfos []*ActivityInfo, timerInfos []*TimerInfo, childExecutionInfos []*ChildExecutionInfo,
	requestCancelInfos []*RequestCancelInfo, signalInfos []*SignalInfo, ids []string) error {
	return s.WorkflowMgr.ResetMutableState(&ResetMutableStateRequest{
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
	})
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(info *WorkflowExecutionInfo) error {
	return s.WorkflowMgr.DeleteWorkflowExecution(&DeleteWorkflowExecutionRequest{
		DomainID:   info.DomainID,
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
	})
}

// GetTransferTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTransferTasks(batchSize int, getAll bool) ([]*TransferTaskInfo, error) {
	result := []*TransferTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.WorkflowMgr.GetTransferTasks(&GetTransferTasksRequest{
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
		atomic.StoreInt64(&s.readLevel, task.TaskID)
	}

	return result, nil
}

// GetReplicationTasks is a utility method to get tasks from replication task queue
func (s *TestBase) GetReplicationTasks(batchSize int, getAll bool) ([]*ReplicationTaskInfo, error) {
	result := []*ReplicationTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.WorkflowMgr.GetReplicationTasks(&GetReplicationTasksRequest{
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
		atomic.StoreInt64(&s.replicationReadLevel, task.TaskID)
	}

	return result, nil
}

// CompleteTransferTask is a utility method to complete a transfer task
func (s *TestBase) CompleteTransferTask(taskID int64) error {

	return s.WorkflowMgr.CompleteTransferTask(&CompleteTransferTaskRequest{
		TaskID: taskID,
	})
}

// RangeCompleteTransferTask is a utility method to complete a range of transfer tasks
func (s *TestBase) RangeCompleteTransferTask(exclusiveBeginTaskID int64, inclusiveEndTaskID int64) error {
	return s.WorkflowMgr.RangeCompleteTransferTask(&RangeCompleteTransferTaskRequest{
		ExclusiveBeginTaskID: exclusiveBeginTaskID,
		InclusiveEndTaskID:   inclusiveEndTaskID,
	})
}

// CompleteReplicationTask is a utility method to complete a replication task
func (s *TestBase) CompleteReplicationTask(taskID int64) error {

	return s.WorkflowMgr.CompleteReplicationTask(&CompleteReplicationTaskRequest{
		TaskID: taskID,
	})
}

// GetTimerIndexTasks is a utility method to get tasks from task queue
func (s *TestBase) GetTimerIndexTasks(batchSize int, getAll bool) ([]*TimerTaskInfo, error) {
	result := []*TimerTaskInfo{}
	var token []byte

Loop:
	for {
		response, err := s.WorkflowMgr.GetTimerIndexTasks(&GetTimerIndexTasksRequest{
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
	return s.WorkflowMgr.CompleteTimerTask(&CompleteTimerTaskRequest{
		VisibilityTimestamp: ts,
		TaskID:              taskID,
	})
}

// RangeCompleteTimerTask is a utility method to complete a range of timer tasks
func (s *TestBase) RangeCompleteTimerTask(inclusiveBeginTimestamp time.Time, exclusiveEndTimestamp time.Time) error {
	return s.WorkflowMgr.RangeCompleteTimerTask(&RangeCompleteTimerTaskRequest{
		InclusiveBeginTimestamp: inclusiveBeginTimestamp,
		ExclusiveEndTimestamp:   exclusiveEndTimestamp,
	})
}

// CreateDecisionTask is a utility method to create a task
func (s *TestBase) CreateDecisionTask(domainID string, workflowExecution workflow.WorkflowExecution, taskList string,
	decisionScheduleID int64) (int64, error) {
	leaseResponse, err := s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: TaskListTypeDecision,
	})
	if err != nil {
		return 0, err
	}

	taskID := s.GetNextSequenceNumber()
	tasks := []*CreateTaskInfo{
		{
			TaskID:    taskID,
			Execution: workflowExecution,
			Data: &TaskInfo{
				DomainID:   domainID,
				WorkflowID: *workflowExecution.WorkflowId,
				RunID:      *workflowExecution.RunId,
				TaskID:     taskID,
				ScheduleID: decisionScheduleID,
			},
		},
	}

	_, err = s.TaskMgr.CreateTasks(&CreateTasksRequest{
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

	var taskIDs []int64
	var leaseResponse *LeaseTaskListResponse
	var err error
	for activityScheduleID, taskList := range activities {

		leaseResponse, err = s.TaskMgr.LeaseTaskList(
			&LeaseTaskListRequest{DomainID: domainID, TaskList: taskList, TaskType: TaskListTypeActivity})
		if err != nil {
			return []int64{}, err
		}
		taskID := s.GetNextSequenceNumber()
		tasks := []*CreateTaskInfo{
			{
				TaskID:    taskID,
				Execution: workflowExecution,
				Data: &TaskInfo{
					DomainID:   domainID,
					WorkflowID: *workflowExecution.WorkflowId,
					RunID:      *workflowExecution.RunId,
					TaskID:     taskID,
					ScheduleID: activityScheduleID,
				},
			},
		}
		_, err := s.TaskMgr.CreateTasks(&CreateTasksRequest{
			TaskListInfo: leaseResponse.TaskListInfo,
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
func (s *TestBase) GetTasks(domainID, taskList string, taskType int, batchSize int) (*GetTasksResponse, error) {
	leaseResponse, err := s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: taskType,
	})
	if err != nil {
		return nil, err
	}

	response, err := s.TaskMgr.GetTasks(&GetTasksRequest{
		DomainID:     domainID,
		TaskList:     taskList,
		TaskType:     taskType,
		BatchSize:    batchSize,
		RangeID:      leaseResponse.TaskListInfo.RangeID,
		MaxReadLevel: math.MaxInt64,
	})

	if err != nil {
		return nil, err
	}

	return &GetTasksResponse{Tasks: response.Tasks}, nil
}

// CompleteTask is a utility method to complete a task
func (s *TestBase) CompleteTask(domainID, taskList string, taskType int, taskID int64, ackLevel int64) error {
	leaseResponse, err := s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: taskType,
	})
	if err != nil {
		return err
	}

	return s.TaskMgr.CompleteTask(&CompleteTaskRequest{
		TaskList: &TaskListInfo{
			DomainID: domainID,
			AckLevel: ackLevel,
			TaskType: taskType,
			Name:     taskList,
			RangeID:  leaseResponse.TaskListInfo.RangeID,
		},
		TaskID: taskID,
	})
}

// SetupWorkflowStore to setup workflow test base
func (s *TestBase) SetupWorkflowStore() {
	s.SetupWorkflowStoreWithOptions(TestBaseOptions{
		SchemaDir:          testSchemaDir,
		ClusterHost:        testWorkflowClusterHosts,
		ClusterPort:        testPort,
		ClusterUser:        testUser,
		ClusterPassword:    testPassword,
		DropKeySpace:       true,
		EnableGlobalDomain: false,
	}, nil)
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	s.CassandraTestCluster.tearDownTestCluster()
}

// GetNextSequenceNumber generates a unique sequence number for can be used for transfer queue taskId
func (s *TestBase) GetNextSequenceNumber() int64 {
	taskID, _ := s.TaskIDGenerator.GetNextTransferTaskID()
	return taskID
}

// GetTransferReadLevel returns the current read level for shard
func (s *TestBase) GetTransferReadLevel() int64 {
	return atomic.LoadInt64(&s.readLevel)
}

// GetReplicationReadLevel returns the current read level for shard
func (s *TestBase) GetReplicationReadLevel() int64 {
	return atomic.LoadInt64(&s.replicationReadLevel)
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
	atomic.StoreInt64(&s.readLevel, 0)
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
	atomic.StoreInt64(&s.replicationReadLevel, 0)
}

func (s *CassandraTestCluster) setupTestCluster(options TestBaseOptions) {
	keySpace := options.KeySpace
	if keySpace == "" {
		keySpace = generateRandomKeyspace(10)
	}
	s.createCluster(
		testWorkflowClusterHosts, options.ClusterPort, testUser, testPassword, testDatacenter,
		gocql.Consistency(1), keySpace,
	)
	s.createKeyspace(1, options.DropKeySpace)
	s.loadSchema([]string{"schema.cql"}, options.SchemaDir)
	s.loadVisibilitySchema([]string{"schema.cql"}, options.SchemaDir)
}

func (s *CassandraTestCluster) tearDownTestCluster() {
	s.dropKeyspace()
	s.session.Close()
}

func (s *CassandraTestCluster) createCluster(
	clusterHosts string, port int, user, password, dc string,
	cons gocql.Consistency, keyspace string) {
	s.cluster = common.NewCassandraCluster(clusterHosts, port, user, password, dc)
	s.cluster.Consistency = cons
	s.cluster.Keyspace = "system"
	s.cluster.Timeout = 40 * time.Second
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		log.WithField(logging.TagErr, err).Fatal(`createSession`)
	}
	s.keyspace = keyspace
}

func (s *CassandraTestCluster) createKeyspace(replicas int, dropKeySpace bool) {
	err := common.CreateCassandraKeyspace(s.session, s.keyspace, replicas, dropKeySpace)
	if err != nil {
		log.Fatal(err)
	}

	s.cluster.Keyspace = s.keyspace
}

func (s *CassandraTestCluster) dropKeyspace() {
	err := common.DropCassandraKeyspace(s.session, s.keyspace)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

func (s *CassandraTestCluster) loadSchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := "./schema/cadence"
	if schemaDir != "" {
		workflowSchemaDir = schemaDir + "/schema/cadence"
	}

	err := common.LoadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Port, s.keyspace, true)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

func (s *CassandraTestCluster) loadVisibilitySchema(fileNames []string, schemaDir string) {
	workflowSchemaDir := "./schema/visibility"
	if schemaDir != "" {
		workflowSchemaDir = schemaDir + "/schema/visibility"
	}

	err := common.LoadCassandraSchema(workflowSchemaDir, fileNames, s.cluster.Port, s.keyspace, false)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
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

func generateRandomKeyspace(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("workflow")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

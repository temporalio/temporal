// Copyright (c) 2016 Uber Technologies, Inc.
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

package host

import (
	"bytes"
	"encoding/binary"
	"github.com/uber/cadence/common/cluster"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

func (s *integrationSuite) TestArchival_NotEnabled() {
	s.Equal(cluster.ArchivalEnabled, s.ClusterMetadata.ArchivalConfig().GetArchivalStatus())

	domainID := uuid.New()
	domain := "archival_domain_not_enabled"
	archivalBucket := s.bucketName
	_, err := s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          domainID,
			Name:        domain,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test domain for archival not enabled integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:      0,
			EmitMetric:     false,
			ArchivalStatus: workflow.ArchivalStatusDisabled,
			ArchivalBucket: archivalBucket,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})
	s.NoError(err)

	getDomainReq := &persistence.GetDomainRequest{
		ID: domainID,
	}
	getDomainResp, err := s.MetadataManager.GetDomain(getDomainReq)
	s.NoError(err)
	s.NotNil(getDomainResp)
	s.Equal(int32(0), getDomainResp.Config.Retention)
	s.Equal(workflow.ArchivalStatusDisabled, getDomainResp.Config.ArchivalStatus)
	s.Equal(archivalBucket, getDomainResp.Config.ArchivalBucket)

	workflowID := "archival-workflow-id"
	workflowType := "archival-workflow-type"
	taskList := "archival-task-list"
	numActivities := 1
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskList, domain, numActivities)

	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}
	s.True(s.historyDeletedFromPersistence(getHistoryReq))
}

func (s *integrationSuite) TestArchival_Enabled() {
	s.Equal(cluster.ArchivalEnabled, s.ClusterMetadata.ArchivalConfig().GetArchivalStatus())

	domainID := uuid.New()
	domain := "archival_domain_enabled"
	archivalBucket := s.bucketName
	_, err := s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          domainID,
			Name:        domain,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test domain for archival enabled integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:      0,
			EmitMetric:     false,
			ArchivalStatus: workflow.ArchivalStatusEnabled,
			ArchivalBucket: archivalBucket,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})
	s.NoError(err)

	getDomainReq := &persistence.GetDomainRequest{
		ID: domainID,
	}
	getDomainResp, err := s.MetadataManager.GetDomain(getDomainReq)
	s.NoError(err)
	s.NotNil(getDomainResp)
	s.Equal(int32(0), getDomainResp.Config.Retention)
	s.Equal(workflow.ArchivalStatusEnabled, getDomainResp.Config.ArchivalStatus)
	s.Equal(archivalBucket, getDomainResp.Config.ArchivalBucket)

	workflowID := "archival-workflow-id"
	workflowType := "archival-workflow-type"
	taskList := "archival-task-list"
	numActivities := 1
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskList, domain, numActivities)

	// getHistoryReq without runID will cause history to attempt to be read from persistence
	// the correct way to do this should be to update dynamic config
	// but there is not currently a good way to do this in test the framework
	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
		},
	}
	s.True(s.historyDeletedFromPersistence(getHistoryReq))
	s.False(s.historyArchived(getHistoryReq))
	// update getHistoryReq to include runID thereby enabling history to be fetched from archival
	getHistoryReq.Execution.RunId = common.StringPtr(runID)
	s.True(s.historyArchived(getHistoryReq))
}

func (s *integrationSuite) historyDeletedFromPersistence(getHistoryReq *workflow.GetWorkflowExecutionHistoryRequest) bool {
	for i := 0; i < 10; i++ {
		_, err := s.engine.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err != nil {
			_, ok := err.(*workflow.EntityNotExistsError)
			return ok
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func (s *integrationSuite) historyArchived(getHistoryReq *workflow.GetWorkflowExecutionHistoryRequest) bool {
	for i := 0; i < 10; i++ {
		getHistoryResp, _ := s.engine.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if getHistoryResp != nil && getHistoryResp.GetArchived() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func (s *integrationSuite) startAndFinishWorkflow(id string, wt string, tl string, domain string, numActivities int) string {
	identity := "worker1"
	activityName := "activity_type1"
	workflowType := &workflow.WorkflowType{
		Name: common.StringPtr(wt),
	}
	taskList := &workflow.TaskList{
		Name: common.StringPtr(tl),
	}
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domain),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)
	s.Logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)

	dtHandler := func(
		execution *workflow.WorkflowExecution,
		wt *workflow.WorkflowType,
		previousStartedEventID int64,
		startedEventID int64,
		history *workflow.History,
	) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}
		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	expectedActivity := int32(1)
	atHandler := func(
		execution *workflow.WorkflowExecution,
		activityType *workflow.ActivityType,
		activityID string,
		input []byte,
		taskToken []byte,
	) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          domain,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}
	for i := 0; i < numActivities; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err)
		if i%2 == 0 {
			err = poller.PollAndProcessActivityTask(false)
		} else { // just for testing respondActivityTaskCompleteByID
			err = poller.PollAndProcessActivityTaskWithID(false)
		}
		s.Logger.Infof("PollAndProcessActivityTask: %v", err)
		s.Nil(err)
	}

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
	return *we.RunId
}

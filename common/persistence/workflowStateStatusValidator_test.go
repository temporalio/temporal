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

package persistence

import (
	"testing"

	"github.com/stretchr/testify/suite"
	executionpb "go.temporal.io/temporal-proto/execution"
)

type (
	workflowStateStatusSuite struct {
		suite.Suite
	}
)

func TestWorkflowStateStatusSuite(t *testing.T) {
	s := new(workflowStateStatusSuite)
	suite.Run(t, s)
}

func (s *workflowStateStatusSuite) SetupSuite() {
}

func (s *workflowStateStatusSuite) TearDownSuite() {

}

func (s *workflowStateStatusSuite) SetupTest() {

}

func (s *workflowStateStatusSuite) TearDownTest() {

}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Running,
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCompleted, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, status))
	}
}

// TODO

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, status))
	}
}

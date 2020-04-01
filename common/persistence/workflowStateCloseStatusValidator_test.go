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
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal-proto/enums"
)

type (
	workflowStateCloseStatusSuite struct {
		suite.Suite
	}
)

func TestWorkflowStateCloseStatusSuite(t *testing.T) {
	s := new(workflowStateCloseStatusSuite)
	suite.Run(t, s)
}

func (s *workflowStateCloseStatusSuite) SetupSuite() {
}

func (s *workflowStateCloseStatusSuite) TearDownSuite() {

}

func (s *workflowStateCloseStatusSuite) SetupTest() {

}

func (s *workflowStateCloseStatusSuite) TearDownTest() {

}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateCreated() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateRunning() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateCompleted() {
	closeStatuses := []int{
		WorkflowCloseStatusRunning,
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCompleted, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestCreateWorkflowStateCloseStatus_WorkflowStateZombie() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

// TODO

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateCreated() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateRunning() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateCompleted() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

func (s *workflowStateCloseStatusSuite) TestUpdateWorkflowStateCloseStatus_WorkflowStateZombie() {
	closeStatuses := []int{
		WorkflowCloseStatusCompleted,
		WorkflowCloseStatusFailed,
		WorkflowCloseStatusCanceled,
		WorkflowCloseStatusTerminated,
		WorkflowCloseStatusContinuedAsNew,
		WorkflowCloseStatusTimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, WorkflowCloseStatusRunning))

	for _, closeStatus := range closeStatuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, enums.WorkflowExecutionCloseStatus(closeStatus)))
	}
}

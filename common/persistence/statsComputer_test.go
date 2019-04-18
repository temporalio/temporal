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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	statsComputerSuite struct {
		sc *statsComputer
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestStatsComputerSuite(t *testing.T) {
	s := new(statsComputerSuite)
	suite.Run(t, s)
}

//TODO need to add more tests
func (s *statsComputerSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.sc = &statsComputer{}
}

func (s *statsComputerSuite) createRequest() *InternalUpdateWorkflowExecutionRequest {
	return &InternalUpdateWorkflowExecutionRequest{
		ExecutionInfo: &InternalWorkflowExecutionInfo{},
	}
}

func (s *statsComputerSuite) TestStatsWithStartedEvent() {
	ms := s.createRequest()
	domainID := "A"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-id"),
		RunId:      common.StringPtr("run_id"),
	}
	workflowType := &workflow.WorkflowType{
		Name: common.StringPtr("test-workflow-type-name"),
	}
	tasklist := &workflow.TaskList{
		Name: common.StringPtr("test-tasklist"),
	}

	ms.ExecutionInfo.DomainID = domainID
	ms.ExecutionInfo.WorkflowID = *execution.WorkflowId
	ms.ExecutionInfo.RunID = *execution.RunId
	ms.ExecutionInfo.WorkflowTypeName = *workflowType.Name
	ms.ExecutionInfo.TaskList = *tasklist.Name

	expectedSize := len(execution.GetWorkflowId()) + len(workflowType.GetName()) + len(tasklist.GetName())

	stats := s.sc.computeMutableStateUpdateStats(ms)
	s.Equal(stats.ExecutionInfoSize, expectedSize)
}

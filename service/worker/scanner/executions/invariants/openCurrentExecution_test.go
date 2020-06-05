// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package invariants

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type OpenCurrentExecutionSuite struct {
	*require.Assertions
	suite.Suite
}

func TestOpenCurrentExecutionSuite(t *testing.T) {
	suite.Run(t, new(OpenCurrentExecutionSuite))
}

func (s *OpenCurrentExecutionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *OpenCurrentExecutionSuite) TestCheck() {
	testCases := []struct {
		execution       common.Execution
		getCurrentResp  *persistence.GetCurrentExecutionResponse
		getCurrentErr   error
		getConcreteResp *persistence.GetWorkflowExecutionResponse
		getConcreteErr  error
		expectedResult  common.CheckResult
	}{
		{
			execution: getClosedExecution(),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
			},
		},
		{
			execution:      getOpenExecution(),
			getConcreteErr: errors.New("got error checking if concrete is open"),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
				Info:            "failed to check if concrete execution is still open",
				InfoDetails:     "got error checking if concrete is open",
			},
		},
		{
			execution: getOpenExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: closedState,
					},
				},
			},
			getConcreteErr: nil,
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
			},
		},
		{
			execution: getOpenExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  &shared.EntityNotExistsError{},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
				Info:            "execution is open without having current execution",
				InfoDetails:     "EntityNotExistsError{Message: }",
			},
		},
		{
			execution: getOpenExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  errors.New("error getting current execution"),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
				Info:            "failed to check if current execution exists",
				InfoDetails:     "error getting current execution",
			},
		},
		{
			execution: getOpenExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  nil,
			getCurrentResp: &persistence.GetCurrentExecutionResponse{
				RunID: "not-equal",
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
				Info:            "execution is open but current points at a different execution",
				InfoDetails:     "current points at not-equal",
			},
		},
		{
			execution: getOpenExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  nil,
			getCurrentResp: &persistence.GetCurrentExecutionResponse{
				RunID: runID,
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.OpenCurrentExecutionInvariantType,
			},
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything).Return(tc.getConcreteResp, tc.getConcreteErr)
		execManager.On("GetCurrentExecution", mock.Anything).Return(tc.getCurrentResp, tc.getCurrentErr)
		o := NewOpenCurrentExecution(common.NewPersistenceRetryer(execManager, nil))
		s.Equal(tc.expectedResult, o.Check(tc.execution))
	}
}

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

package invariant

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	c2 "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
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
		execution       *entity.ConcreteExecution
		getCurrentResp  *persistence.GetCurrentExecutionResponse
		getCurrentErr   error
		getConcreteResp *persistence.GetWorkflowExecutionResponse
		getConcreteErr  error
		expectedResult  CheckResult
	}{
		{
			execution: getClosedConcreteExecution(),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
		{
			execution:      getOpenConcreteExecution(),
			getConcreteErr: errors.New("got error checking if concrete is open"),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   OpenCurrentExecution,
				Info:            "failed to check if concrete execution is still open",
				InfoDetails:     "got error checking if concrete is open",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: closedState,
					},
				},
			},
			getConcreteErr: nil,
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  &shared.EntityNotExistsError{},
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   OpenCurrentExecution,
				Info:            "execution is open without having current execution",
				InfoDetails:     "EntityNotExistsError{Message: }",
			},
		},
		{
			execution: getOpenConcreteExecution(),
			getConcreteResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: openState,
					},
				},
			},
			getConcreteErr: nil,
			getCurrentErr:  errors.New("error getting current execution"),
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   OpenCurrentExecution,
				Info:            "failed to check if current execution exists",
				InfoDetails:     "error getting current execution",
			},
		},
		{
			execution: getOpenConcreteExecution(),
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
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   OpenCurrentExecution,
				Info:            "execution is open but current points at a different execution",
				InfoDetails:     "current points at not-equal",
			},
		},
		{
			execution: getOpenConcreteExecution(),
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
			expectedResult: CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   OpenCurrentExecution,
			},
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything).Return(tc.getConcreteResp, tc.getConcreteErr)
		execManager.On("GetCurrentExecution", mock.Anything).Return(tc.getCurrentResp, tc.getCurrentErr)
		o := NewOpenCurrentExecution(persistence.NewPersistenceRetryer(execManager, nil, c2.CreatePersistenceRetryPolicy()))
		s.Equal(tc.expectedResult, o.Check(tc.execution))
	}
}

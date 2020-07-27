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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/common"
)

type ConcreteExecutionExistsSuite struct {
	*require.Assertions
	suite.Suite
}

func TestConcreteExecutionExistsSuite(t *testing.T) {
	suite.Run(t, new(ConcreteExecutionExistsSuite))
}

func (s *ConcreteExecutionExistsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *ConcreteExecutionExistsSuite) TestCheck() {
	testCases := []struct {
		execution       *common.CurrentExecution
		getConcreteResp *persistence.IsWorkflowExecutionExistsResponse
		getConcreteErr  error
		expectedResult  common.CheckResult
	}{
		{
			execution: getClosedCurrentExecution(),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.ConcreteExecutionExistsInvariantType,
			},
		},
		{
			execution:      getOpenCurrentExecution(),
			getConcreteErr: errors.New("error getting concrete execution"),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   common.ConcreteExecutionExistsInvariantType,
				Info:            "failed to check if concrete execution exists",
				InfoDetails:     "error getting concrete execution",
			},
		},
		{
			execution:       getOpenCurrentExecution(),
			getConcreteResp: &persistence.IsWorkflowExecutionExistsResponse{Exists: false},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.ConcreteExecutionExistsInvariantType,
				Info:            "execution is open without having concrete execution",
				InfoDetails: fmt.Sprintf("concrete execution not found. WorkflowId: %v, RunId: %v",
					workflowID, currentRunID),
			},
		},
		{
			execution:       getOpenCurrentExecution(),
			getConcreteErr:  nil,
			getConcreteResp: &persistence.IsWorkflowExecutionExistsResponse{Exists: true},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.ConcreteExecutionExistsInvariantType,
			},
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("IsWorkflowExecutionExists", mock.Anything).Return(tc.getConcreteResp, tc.getConcreteErr)
		o := NewConcreteExecutionExists(common.NewPersistenceRetryer(execManager, nil))
		s.Equal(tc.expectedResult, o.Check(tc.execution))
	}
}

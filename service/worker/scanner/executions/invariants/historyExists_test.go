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

type HistoryExistsSuite struct {
	*require.Assertions
	suite.Suite
}

func TestHistoryExistsSuite(t *testing.T) {
	suite.Run(t, new(HistoryExistsSuite))
}

func (s *HistoryExistsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HistoryExistsSuite) TestCheck() {
	testCases := []struct {
		getExecErr                error
		getExecResp               *persistence.GetWorkflowExecutionResponse
		getHistoryErr             error
		getHistoryResp            *persistence.ReadHistoryBranchResponse
		expectedResult            common.CheckResult
		expectedResourcePopulated bool
	}{
		{
			getExecErr:     errors.New("got error checking workflow exists"),
			getHistoryResp: &persistence.ReadHistoryBranchResponse{},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   common.HistoryExistsInvariantType,
				Info:            "failed to check if concrete execution still exists",
				InfoDetails:     "got error checking workflow exists",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecErr:     &shared.EntityNotExistsError{},
			getHistoryResp: &persistence.ReadHistoryBranchResponse{},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.HistoryExistsInvariantType,
				Info:            "determined execution was healthy because concrete execution no longer exists",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			getHistoryErr:  &shared.EntityNotExistsError{Message: "got entity not exists error"},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.HistoryExistsInvariantType,
				Info:            "concrete execution exists but history does not exist",
				InfoDetails:     "EntityNotExistsError{Message: got entity not exists error}",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			getHistoryErr:  errors.New("error fetching history"),
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   common.HistoryExistsInvariantType,
				Info:            "failed to verify if history exists",
				InfoDetails:     "error fetching history",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp:    &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: nil,
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.HistoryExistsInvariantType,
				Info:            "concrete execution exists but got empty history",
			},
			expectedResourcePopulated: false,
		},
		{
			getExecResp: &persistence.GetWorkflowExecutionResponse{},
			getHistoryResp: &persistence.ReadHistoryBranchResponse{
				HistoryEvents: []*shared.HistoryEvent{
					{},
				},
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.HistoryExistsInvariantType,
			},
			expectedResourcePopulated: true,
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		historyManager := &mocks.HistoryV2Manager{}
		execManager.On("GetWorkflowExecution", mock.Anything).Return(tc.getExecResp, tc.getExecErr)
		historyManager.On("ReadHistoryBranch", mock.Anything).Return(tc.getHistoryResp, tc.getHistoryErr)
		i := NewHistoryExists(common.NewPersistenceRetryer(execManager, historyManager))
		resources := &common.InvariantResourceBag{}
		result := i.Check(getOpenExecution(), resources)
		s.Equal(tc.expectedResult, result)
		if tc.expectedResourcePopulated {
			s.NotNil(resources.History)
		} else {
			s.Nil(resources.History)
		}
	}
}

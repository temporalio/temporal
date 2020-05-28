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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type ValidFirstEventSuite struct {
	*require.Assertions
	suite.Suite
}

func TestValidFirstEventSuite(t *testing.T) {
	suite.Run(t, new(ValidFirstEventSuite))
}

func (s *ValidFirstEventSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *ValidFirstEventSuite) TestCheck() {
	testCases := []struct {
		firstEvent     *shared.HistoryEvent
		expectedResult common.CheckResult
	}{
		{
			firstEvent: &shared.HistoryEvent{
				EventId: c.Int64Ptr(10),
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.ValidFirstEventInvariantType,
				Info:            "got unexpected first eventID",
				InfoDetails:     "expected 1 but got 10",
			},
		},
		{
			firstEvent: &shared.HistoryEvent{
				EventId:   c.Int64Ptr(1),
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   common.ValidFirstEventInvariantType,
				Info:            "got unexpected first event type",
				InfoDetails:     "expected WorkflowExecutionStarted but got WorkflowExecutionCanceled",
			},
		},
		{
			firstEvent: &shared.HistoryEvent{
				EventId:   c.Int64Ptr(1),
				EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
			},
			expectedResult: common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   common.ValidFirstEventInvariantType,
			},
		},
	}

	for _, tc := range testCases {
		v := NewValidFirstEvent(nil)
		result := v.Check(common.Execution{}, &common.InvariantResourceBag{
			History: &persistence.ReadHistoryBranchResponse{
				HistoryEvents: []*shared.HistoryEvent{
					tc.firstEvent,
				},
			},
		})
		s.Equal(tc.expectedResult, result)
	}
}

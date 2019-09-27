// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
)

type QuerySuite struct {
	*require.Assertions
	suite.Suite
}

func TestQuerySuite(t *testing.T) {
	suite.Run(t, new(QuerySuite))
}

func (s *QuerySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *QuerySuite) TestCompletedTerminalState() {
	testCases := []struct {
		event  queryEvent
		result *shared.WorkflowQueryResult

		expectChanged        bool
		expectErr            error
		expectState          queryState
		expectTermChClosed   bool
		expectResultRecorded bool
	}{
		{
			event:  queryEventRebuffer,
			result: nil,

			expectChanged:        false,
			expectErr:            errInvalidEvent,
			expectState:          queryStateBuffered,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventRecordResult,
			result: nil,

			expectChanged:        false,
			expectErr:            errInvalidEvent,
			expectState:          queryStateBuffered,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventStart,
			result: nil,

			expectChanged:        true,
			expectErr:            nil,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventRebuffer,
			result: nil,

			expectChanged:        true,
			expectErr:            nil,
			expectState:          queryStateBuffered,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventStart,
			result: nil,

			expectChanged:        true,
			expectErr:            nil,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventRebuffer,
			result: &shared.WorkflowQueryResult{},

			expectChanged:        false,
			expectErr:            errInvalidEvent,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventRecordResult,
			result: nil,

			expectChanged:        false,
			expectErr:            errInvalidEvent,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: false,
		},
		{
			event:  queryEventRecordResult,
			result: &shared.WorkflowQueryResult{},

			expectChanged:        false,
			expectErr:            nil,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: true,
		},
		{
			event:  queryEventRebuffer,
			result: nil,

			expectChanged:        false,
			expectErr:            errResultAlreadyRecorded,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: true,
		},
		{
			event:  queryEventRecordResult,
			result: &shared.WorkflowQueryResult{},

			expectChanged:        false,
			expectErr:            errResultAlreadyRecorded,
			expectState:          queryStateStarted,
			expectTermChClosed:   false,
			expectResultRecorded: true,
		},
		{
			event:  queryEventPersistenceConditionSatisfied,
			result: nil,

			expectChanged:        true,
			expectErr:            nil,
			expectState:          queryStateCompleted,
			expectTermChClosed:   true,
			expectResultRecorded: true,
		},
		{
			event:  queryEventPersistenceConditionSatisfied,
			result: nil,

			expectChanged:        false,
			expectErr:            errAlreadyCompleted,
			expectState:          queryStateCompleted,
			expectTermChClosed:   true,
			expectResultRecorded: true,
		},
	}

	qsm := newQueryStateMachine(&shared.WorkflowQuery{})
	for _, tc := range testCases {
		changed, err := qsm.recordEvent(tc.event, tc.result)
		s.Equal(tc.expectChanged, changed)
		s.Equal(tc.expectErr, err)
		s.Equal(tc.expectState, qsm.getQuerySnapshot().state)
		s.Equal(tc.expectTermChClosed, s.chanClosed(qsm.getQueryTermCh()))
		if tc.expectResultRecorded {
			s.NotNil(qsm.getQuerySnapshot().queryResult)
		} else {
			s.Nil(qsm.getQuerySnapshot().queryResult)
		}
	}
}

func (s *QuerySuite) chanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

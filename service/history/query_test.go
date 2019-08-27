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

func (s *QuerySuite) TestExpiredTerminalState() {
	query := NewQuery(&shared.WorkflowQuery{})
	s.False(s.chanClosed(query.TerminationCh()))
	changed, err := query.RecordEvent(QueryEventExpire, nil)
	s.NoError(err)
	s.True(changed)
	s.True(s.chanClosed(query.TerminationCh()))
	s.Equal(QueryStateExpired, query.State())
	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.Equal(ErrAlreadyTerminal, err)
	s.False(changed)
}

func (s *QuerySuite) TestCompletedTerminalState() {
	query := NewQuery(&shared.WorkflowQuery{})

	changed, err := query.RecordEvent(QueryEventRebuffer, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRecordResult, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRebuffer, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRebuffer, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRecordResult, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRebuffer, nil)
	s.False(changed)
	s.Equal(ErrResultAlreadyRecorded, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.Equal(ErrResultAlreadyRecorded, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateCompleted, query.State())
	s.True(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())

	changed, err = query.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
	s.False(changed)
	s.Equal(ErrAlreadyTerminal, err)
	s.Equal(QueryStateCompleted, query.State())
	s.True(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
}

func (s *QuerySuite) chanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

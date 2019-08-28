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
	"errors"
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
	qrc := queryRegistryCallbacks{}
	query := newQuery(&shared.WorkflowQuery{}, qrc.bufferedToStartedCallback, qrc.startedToBufferedCallback, qrc.terminalCallback)
	s.False(s.chanClosed(query.TerminationCh()))
	changed, err := query.RecordEvent(QueryEventExpire, nil)
	s.NoError(err)
	s.True(changed)
	s.True(s.chanClosed(query.TerminationCh()))
	s.Equal(QueryStateExpired, query.State())
	s.True(qrc.statesMatch(0, 0, 1))
	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.Equal(ErrAlreadyTerminal, err)
	s.False(changed)
}

func (s *QuerySuite) TestCompletedTerminalState() {
	qrc := queryRegistryCallbacks{}
	query := newQuery(&shared.WorkflowQuery{}, qrc.bufferedToStartedCallback, qrc.startedToBufferedCallback, qrc.terminalCallback)

	changed, err := query.RecordEvent(QueryEventRebuffer, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(0, 0, 0))

	changed, err = query.RecordEvent(QueryEventRecordResult, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(0, 0, 0))

	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(1, 0, 0))

	changed, err = query.RecordEvent(QueryEventRebuffer, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(1, 1, 0))

	changed, err = query.RecordEvent(QueryEventStart, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventRebuffer, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventRecordResult, nil)
	s.False(changed)
	s.Equal(ErrInvalidEvent, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.Nil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.NoError(err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventRebuffer, nil)
	s.False(changed)
	s.Equal(ErrResultAlreadyRecorded, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
	s.False(changed)
	s.Equal(ErrResultAlreadyRecorded, err)
	s.Equal(QueryStateStarted, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 0))

	changed, err = query.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
	s.True(changed)
	s.NoError(err)
	s.Equal(QueryStateCompleted, query.State())
	s.True(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 1))

	changed, err = query.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
	s.False(changed)
	s.Equal(ErrAlreadyTerminal, err)
	s.Equal(QueryStateCompleted, query.State())
	s.True(s.chanClosed(query.TerminationCh()))
	s.NotNil(query.QueryResult())
	s.True(qrc.statesMatch(2, 1, 1))
}

func (s *QuerySuite) TestCallbackFailed() {
	query := newQuery(&shared.WorkflowQuery{}, func(_ string) error { return errors.New("error") }, nil, nil)
	changed, err := query.RecordEvent(QueryEventStart, nil)
	s.Equal(ErrFailedToInvokeQueryRegistryCallback, err)
	s.False(changed)
	s.Equal(QueryStateBuffered, query.State())
	s.False(s.chanClosed(query.TerminationCh()))
}

func (s *QuerySuite) chanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

type queryRegistryCallbacks struct {
	bufferedToStartedCount int
	startedToBufferedCount int
	terminalStateCount     int
}

func (qrc *queryRegistryCallbacks) bufferedToStartedCallback(_ string) error {
	qrc.bufferedToStartedCount++
	return nil
}

func (qrc *queryRegistryCallbacks) startedToBufferedCallback(_ string) error {
	qrc.startedToBufferedCount++
	return nil
}

func (qrc *queryRegistryCallbacks) terminalCallback(_ string) {
	qrc.terminalStateCount++
}

func (qrc *queryRegistryCallbacks) statesMatch(bufferedToStartedCount int, startedToBufferedCount int, terminalStateCount int) bool {
	return bufferedToStartedCount == qrc.bufferedToStartedCount &&
		startedToBufferedCount == qrc.startedToBufferedCount &&
		terminalStateCount == qrc.terminalStateCount
}

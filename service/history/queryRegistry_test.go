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

	"github.com/uber/cadence/common"

	"github.com/uber/cadence/.gen/go/shared"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type QueryRegistrySuite struct {
	suite.Suite
	*require.Assertions
}

func TestQueryRegistrySuite(t *testing.T) {
	suite.Run(t, new(QueryRegistrySuite))
}

func (s *QueryRegistrySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *QueryRegistrySuite) TestQueryRegistry() {
	qr := newQueryRegistry()
	ids := make([]string, 100, 100)
	termChans := make([]<-chan struct{}, 100, 100)
	for i := 0; i < 100; i++ {
		ids[i], termChans[i] = qr.bufferQuery(&shared.WorkflowQuery{})
	}
	s.assertBufferedState(qr, ids...)
	s.assertHasQueries(qr, true, false, false, false)
	s.assertQuerySizes(qr, 100, 0, 0, 0)
	s.assertChanState(false, termChans...)

	for i := 0; i < 25; i++ {
		err := qr.setTerminationState(ids[i], &queryTerminationState{
			queryTerminationType: queryTerminationTypeCompleted,
			queryResult: &shared.WorkflowQueryResult{
				ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
				Answer:     []byte{1, 2, 3},
			},
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertBufferedState(qr, ids[25:]...)
	s.assertHasQueries(qr, true, true, false, false)
	s.assertQuerySizes(qr, 75, 25, 0, 0)
	s.assertChanState(true, termChans[0:25]...)
	s.assertChanState(false, termChans[25:]...)

	for i := 25; i < 50; i++ {
		err := qr.setTerminationState(ids[i], &queryTerminationState{
			queryTerminationType: queryTerminationTypeUnblocked,
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertBufferedState(qr, ids[50:]...)
	s.assertHasQueries(qr, true, true, true, false)
	s.assertQuerySizes(qr, 50, 25, 25, 0)
	s.assertChanState(true, termChans[0:50]...)
	s.assertChanState(false, termChans[50:]...)

	for i := 50; i < 75; i++ {
		err := qr.setTerminationState(ids[i], &queryTerminationState{
			queryTerminationType: queryTerminationTypeFailed,
			failure:              errors.New("err"),
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertFailedState(qr, ids[50:75]...)
	s.assertBufferedState(qr, ids[75:]...)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 25, 25, 25, 25)
	s.assertChanState(true, termChans[0:75]...)
	s.assertChanState(false, termChans[75:]...)

	for i := 0; i < 75; i++ {
		switch i % 3 {
		case 0:
			s.Equal(errQueryNotExists, qr.setTerminationState(ids[i], &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult:          &shared.WorkflowQueryResult{},
			}))
		case 1:
			s.Equal(errQueryNotExists, qr.setTerminationState(ids[i], &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
			}))
		case 2:
			s.Equal(errQueryNotExists, qr.setTerminationState(ids[i], &queryTerminationState{
				queryTerminationType: queryTerminationTypeFailed,
				failure:              errors.New("err"),
			}))
		}
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertFailedState(qr, ids[50:75]...)
	s.assertBufferedState(qr, ids[75:]...)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 25, 25, 25, 25)
	s.assertChanState(true, termChans[0:75]...)
	s.assertChanState(false, termChans[75:]...)

	for i := 0; i < 25; i++ {
		qr.removeQuery(ids[i])
		s.assertHasQueries(qr, true, i < 24, true, true)
		s.assertQuerySizes(qr, 25, 25-i-1, 25, 25)
	}
	for i := 25; i < 50; i++ {
		qr.removeQuery(ids[i])
		s.assertHasQueries(qr, true, false, i < 49, true)
		s.assertQuerySizes(qr, 25, 0, 50-i-1, 25)
	}
	for i := 50; i < 75; i++ {
		qr.removeQuery(ids[i])
		s.assertHasQueries(qr, true, false, false, i < 74)
		s.assertQuerySizes(qr, 25, 0, 0, 75-i-1)
	}
	for i := 75; i < 100; i++ {
		qr.removeQuery(ids[i])
		s.assertHasQueries(qr, i < 99, false, false, false)
		s.assertQuerySizes(qr, 100-i-1, 0, 0, 0)
	}
	s.assertChanState(true, termChans[0:75]...)
	s.assertChanState(false, termChans[75:]...)
}

func (s *QueryRegistrySuite) assertBufferedState(qr queryRegistry, ids ...string) {
	for _, id := range ids {
		termCh, err := qr.getQueryTermCh(id)
		s.NoError(err)
		s.False(closed(termCh))
		input, err := qr.getQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		termState, err := qr.getTerminationState(id)
		s.Equal(errQueryNotInTerminalState, err)
		s.Nil(termState)
	}
}

func (s *QueryRegistrySuite) assertCompletedState(qr queryRegistry, ids ...string) {
	for _, id := range ids {
		termCh, err := qr.getQueryTermCh(id)
		s.NoError(err)
		s.True(closed(termCh))
		input, err := qr.getQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		termState, err := qr.getTerminationState(id)
		s.NoError(err)
		s.NotNil(termState)
		s.Equal(queryTerminationTypeCompleted, termState.queryTerminationType)
		s.NotNil(termState.queryResult)
		s.Nil(termState.failure)
	}
}

func (s *QueryRegistrySuite) assertUnblockedState(qr queryRegistry, ids ...string) {
	for _, id := range ids {
		termCh, err := qr.getQueryTermCh(id)
		s.NoError(err)
		s.True(closed(termCh))
		input, err := qr.getQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		termState, err := qr.getTerminationState(id)
		s.NoError(err)
		s.NotNil(termState)
		s.Equal(queryTerminationTypeUnblocked, termState.queryTerminationType)
		s.Nil(termState.queryResult)
		s.Nil(termState.failure)
	}
}

func (s *QueryRegistrySuite) assertFailedState(qr queryRegistry, ids ...string) {
	for _, id := range ids {
		termCh, err := qr.getQueryTermCh(id)
		s.NoError(err)
		s.True(closed(termCh))
		input, err := qr.getQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		termState, err := qr.getTerminationState(id)
		s.NoError(err)
		s.NotNil(termState)
		s.Equal(queryTerminationTypeFailed, termState.queryTerminationType)
		s.Nil(termState.queryResult)
		s.NotNil(termState.failure)
	}
}

func (s *QueryRegistrySuite) assertHasQueries(qr queryRegistry, buffered, completed, unblocked, failed bool) {
	s.Equal(buffered, qr.hasBufferedQuery())
	s.Equal(completed, qr.hasCompletedQuery())
	s.Equal(unblocked, qr.hasUnblockedQuery())
	s.Equal(failed, qr.hasFailedQuery())
}

func (s *QueryRegistrySuite) assertQuerySizes(qr queryRegistry, buffered, completed, unblocked, failed int) {
	s.Len(qr.getBufferedIDs(), buffered)
	s.Len(qr.getCompletedIDs(), completed)
	s.Len(qr.getUnblockedIDs(), unblocked)
	s.Len(qr.getFailedIDs(), failed)
}

func (s *QueryRegistrySuite) assertChanState(expectedClosed bool, chans ...<-chan struct{}) {
	for _, ch := range chans {
		s.Equal(expectedClosed, closed(ch))
	}
}

// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"

	"go.temporal.io/server/common/payloads"
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
	qr := NewQueryRegistry()
	ids := make([]string, 100)
	completionChs := make([]<-chan struct{}, 100)
	for i := 0; i < 100; i++ {
		ids[i], completionChs[i] = qr.BufferQuery(&querypb.WorkflowQuery{})
	}
	s.assertBufferedState(qr, ids...)
	s.assertHasQueries(qr, true, false, false, false)
	s.assertQuerySizes(qr, 100, 0, 0, 0)
	s.assertChanState(false, completionChs...)

	for i := 0; i < 25; i++ {
		err := qr.SetCompletionState(ids[i], &QueryCompletionState{
			Type: QueryCompletionTypeSucceeded,
			Result: &querypb.WorkflowQueryResult{
				ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
			},
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertBufferedState(qr, ids[25:]...)
	s.assertHasQueries(qr, true, true, false, false)
	s.assertQuerySizes(qr, 75, 25, 0, 0)
	s.assertChanState(true, completionChs[0:25]...)
	s.assertChanState(false, completionChs[25:]...)

	for i := 25; i < 50; i++ {
		err := qr.SetCompletionState(ids[i], &QueryCompletionState{
			Type: QueryCompletionTypeUnblocked,
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertBufferedState(qr, ids[50:]...)
	s.assertHasQueries(qr, true, true, true, false)
	s.assertQuerySizes(qr, 50, 25, 25, 0)
	s.assertChanState(true, completionChs[0:50]...)
	s.assertChanState(false, completionChs[50:]...)

	for i := 50; i < 75; i++ {
		err := qr.SetCompletionState(ids[i], &QueryCompletionState{
			Type: QueryCompletionTypeFailed,
			Err:  errors.New("err"),
		})
		s.NoError(err)
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertFailedState(qr, ids[50:75]...)
	s.assertBufferedState(qr, ids[75:]...)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 25, 25, 25, 25)
	s.assertChanState(true, completionChs[0:75]...)
	s.assertChanState(false, completionChs[75:]...)

	for i := 0; i < 75; i++ {
		switch i % 3 {
		case 0:
			s.Equal(errQueryNotExists, qr.SetCompletionState(ids[i], &QueryCompletionState{
				Type:   QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{},
			}))
		case 1:
			s.Equal(errQueryNotExists, qr.SetCompletionState(ids[i], &QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
			}))
		case 2:
			s.Equal(errQueryNotExists, qr.SetCompletionState(ids[i], &QueryCompletionState{
				Type: QueryCompletionTypeFailed,
				Err:  errors.New("err"),
			}))
		}
	}
	s.assertCompletedState(qr, ids[0:25]...)
	s.assertUnblockedState(qr, ids[25:50]...)
	s.assertFailedState(qr, ids[50:75]...)
	s.assertBufferedState(qr, ids[75:]...)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 25, 25, 25, 25)
	s.assertChanState(true, completionChs[0:75]...)
	s.assertChanState(false, completionChs[75:]...)

	for i := 0; i < 25; i++ {
		qr.RemoveQuery(ids[i])
		s.assertHasQueries(qr, true, i < 24, true, true)
		s.assertQuerySizes(qr, 25, 25-i-1, 25, 25)
	}
	for i := 25; i < 50; i++ {
		qr.RemoveQuery(ids[i])
		s.assertHasQueries(qr, true, false, i < 49, true)
		s.assertQuerySizes(qr, 25, 0, 50-i-1, 25)
	}
	for i := 50; i < 75; i++ {
		qr.RemoveQuery(ids[i])
		s.assertHasQueries(qr, true, false, false, i < 74)
		s.assertQuerySizes(qr, 25, 0, 0, 75-i-1)
	}
	for i := 75; i < 100; i++ {
		qr.RemoveQuery(ids[i])
		s.assertHasQueries(qr, i < 99, false, false, false)
		s.assertQuerySizes(qr, 100-i-1, 0, 0, 0)
	}
	s.assertChanState(true, completionChs[0:75]...)
	s.assertChanState(false, completionChs[75:]...)
}

func (s *QueryRegistrySuite) assertBufferedState(qr QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		s.NoError(err)
		s.False(closed(completionCh))
		input, err := qr.GetQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		completionState, err := qr.GetCompletionState(id)
		s.Equal(errQueryNotInCompletionState, err)
		s.Nil(completionState)
	}
}

func (s *QueryRegistrySuite) assertCompletedState(qr QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		s.NoError(err)
		s.True(closed(completionCh))
		input, err := qr.GetQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		completionState, err := qr.GetCompletionState(id)
		s.NoError(err)
		s.NotNil(completionState)
		s.Equal(QueryCompletionTypeSucceeded, completionState.Type)
		s.NotNil(completionState.Result)
		s.Nil(completionState.Err)
	}
}

func (s *QueryRegistrySuite) assertUnblockedState(qr QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		s.NoError(err)
		s.True(closed(completionCh))
		input, err := qr.GetQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		completionState, err := qr.GetCompletionState(id)
		s.NoError(err)
		s.NotNil(completionState)
		s.Equal(QueryCompletionTypeUnblocked, completionState.Type)
		s.Nil(completionState.Result)
		s.Nil(completionState.Err)
	}
}

func (s *QueryRegistrySuite) assertFailedState(qr QueryRegistry, ids ...string) {
	for _, id := range ids {
		completionCh, err := qr.GetQueryCompletionCh(id)
		s.NoError(err)
		s.True(closed(completionCh))
		input, err := qr.GetQueryInput(id)
		s.NoError(err)
		s.NotNil(input)
		completionState, err := qr.GetCompletionState(id)
		s.NoError(err)
		s.NotNil(completionState)
		s.Equal(QueryCompletionTypeFailed, completionState.Type)
		s.Nil(completionState.Result)
		s.NotNil(completionState.Err)
	}
}

func (s *QueryRegistrySuite) assertHasQueries(qr QueryRegistry, buffered, completed, unblocked, failed bool) {
	s.Equal(buffered, qr.HasBufferedQuery())
	s.Equal(completed, qr.HasCompletedQuery())
	s.Equal(unblocked, qr.HasUnblockedQuery())
	s.Equal(failed, qr.HasFailedQuery())
}

func (s *QueryRegistrySuite) assertQuerySizes(qr QueryRegistry, buffered, completed, unblocked, failed int) {
	s.Len(qr.GetBufferedIDs(), buffered)
	s.Len(qr.GetCompletedIDs(), completed)
	s.Len(qr.GetUnblockedIDs(), unblocked)
	s.Len(qr.GetFailedIDs(), failed)
}

func (s *QueryRegistrySuite) assertChanState(expectedClosed bool, chans ...<-chan struct{}) {
	for _, ch := range chans {
		s.Equal(expectedClosed, closed(ch))
	}
}

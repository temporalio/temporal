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

	"github.com/uber/cadence/common"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
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
	var ids []string
	for i := 0; i < 10; i++ {
		id, _ := qr.bufferQuery(&shared.WorkflowQuery{})
		ids = append(ids, id)
	}
	s.assertHasQueries(qr, true, false)
	s.assertQuerySizes(qr, 10, 0)

	found, err := qr.getQueryInternalState(ids[0])
	s.NoError(err)
	s.NotNil(found)
	notFound, err := qr.getQueryInternalState("not_exists")
	s.Error(err)
	s.Nil(notFound)

	for i := 0; i < 5; i++ {
		err := qr.completeQuery(ids[i], &shared.WorkflowQueryResult{
			ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
			Answer:     []byte{1, 2, 3},
		})
		s.NoError(err)
	}
	s.assertHasQueries(qr, true, true)
	s.assertQuerySizes(qr, 5, 5)

	for i := 0; i < 5; i++ {
		qs, err := qr.getQueryInternalState(ids[i])
		s.NoError(err)
		s.NotNil(qs)
		s.Equal(queryStateCompleted, qs.state)
		s.NotNil(qs.queryResult)

		termCh, err := qr.getQueryTermCh(ids[i])
		s.NoError(err)
		select {
		case <-termCh:
		default:
			s.Fail("termination channel should be closed")
		}

		qr.removeQuery(ids[i])
	}
	s.assertHasQueries(qr, true, false)
	s.assertQuerySizes(qr, 5, 0)
}

func (s *QueryRegistrySuite) assertHasQueries(qr queryRegistry, buffered, completed bool) {
	s.Equal(buffered, qr.hasBuffered())
	s.Equal(completed, qr.hasCompleted())
}

func (s *QueryRegistrySuite) assertQuerySizes(qr queryRegistry, buffered, completed int) {
	s.Len(qr.getBufferedSnapshot(), buffered)
	s.Len(qr.getCompletedSnapshot(), completed)
}

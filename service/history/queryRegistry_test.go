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

type QueryRegistrySuite struct {
	*require.Assertions
	suite.Suite
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
		id, _, _ := qr.bufferQuery(&shared.WorkflowQuery{})
		ids = append(ids, id)
	}
	s.assertHasQueries(qr, true, false, false, false)
	s.assertQuerySizes(qr, 10, 0, 0, 0)

	found, err := qr.getQuerySnapshot(ids[0])
	s.NoError(err)
	s.NotNil(found)
	notFound, err := qr.getQuerySnapshot("not_exists")
	s.Error(err)
	s.Nil(notFound)

	for i := 0; i < 5; i++ {
		changed, err := qr.recordEvent(ids[i], queryEventStart, nil)
		s.True(changed)
		s.NoError(err)
	}
	s.assertHasQueries(qr, true, true, false, false)
	s.assertQuerySizes(qr, 5, 5, 0, 0)

	completeQuery := func(id string) {
		querySnapshot, err := qr.getQuerySnapshot(id)
		s.NoError(err)
		if querySnapshot.state == queryStateBuffered {
			changed, err := qr.recordEvent(id, queryEventStart, nil)
			s.True(changed)
			s.NoError(err)
		}
		changed, err := qr.recordEvent(id, queryEventPersistenceConditionSatisfied, nil)
		s.False(changed)
		s.NoError(err)
		changed, err = qr.recordEvent(id, queryEventRecordResult, &shared.WorkflowQueryResult{})
		s.True(changed)
		s.NoError(err)
	}

	expireQuery := func(id string) {
		changed, err := qr.recordEvent(id, queryEventExpire, nil)
		s.True(changed)
		s.NoError(err)
	}

	q0, err := qr.getQuerySnapshot(ids[0])
	s.NoError(err)
	s.NotNil(q0)
	s.Equal(queryStateStarted, q0.state)
	completeQuery(q0.id)
	q0, err = qr.getQuerySnapshot(q0.id)
	s.NotNil(q0)
	s.NoError(err)
	s.Equal(queryStateCompleted, q0.state)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 5, 4, 1, 0)

	q1, err := qr.getQuerySnapshot(ids[1])
	s.NoError(err)
	s.NotNil(q1)
	s.Equal(queryStateStarted, q1.state)
	expireQuery(q1.id)
	q1, err = qr.getQuerySnapshot(q1.id)
	s.NotNil(q1)
	s.NoError(err)
	s.Equal(queryStateExpired, q1.state)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 5, 3, 1, 1)

	q9, err := qr.getQuerySnapshot(ids[9])
	s.NoError(err)
	s.NotNil(q9)
	s.Equal(queryStateBuffered, q9.state)
	completeQuery(q9.id)
	q9, err = qr.getQuerySnapshot(q9.id)
	s.NotNil(q9)
	s.NoError(err)
	s.Equal(queryStateCompleted, q9.state)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 4, 3, 2, 1)

	q8, err := qr.getQuerySnapshot(ids[8])
	s.NoError(err)
	s.NotNil(q8)
	s.Equal(queryStateBuffered, q8.state)
	expireQuery(q8.id)
	q8, err = qr.getQuerySnapshot(q8.id)
	s.NotNil(q8)
	s.NoError(err)
	s.Equal(queryStateExpired, q8.state)
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 3, 3, 2, 2)

	qr.removeQuery(ids[0])
	qr.removeQuery(ids[1])
	qr.removeQuery(ids[2])
	qr.removeQuery(ids[5])
	s.assertHasQueries(qr, true, true, true, true)
	s.assertQuerySizes(qr, 2, 2, 1, 1)
}

func (s *QueryRegistrySuite) assertHasQueries(qr queryRegistry, buffered, started, completed, expired bool) {
	s.Equal(buffered, qr.hasBuffered())
	s.Equal(started, qr.hasStarted())
	s.Equal(completed, qr.hasCompleted())
	s.Equal(expired, qr.hasCompleted())
}

func (s *QueryRegistrySuite) assertQuerySizes(qr queryRegistry, buffered, started, completed, expired int) {
	s.Len(qr.getBufferedSnapshot(), buffered)
	s.Len(qr.getStartedSnapshot(), started)
	s.Len(qr.getCompleted(), completed)
	s.Len(qr.getExpired(), expired)
}

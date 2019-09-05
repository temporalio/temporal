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
	qr := NewQueryRegistry()
	var queries []Query
	for i := 0; i < 10; i++ {
		queries = append(queries, qr.BufferQuery(&shared.WorkflowQuery{}))
	}
	s.Len(qr.GetBuffered(), 10)
	s.Len(qr.GetStarted(), 0)
	for i := 0; i < 5; i++ {
		changed, err := queries[i].RecordEvent(QueryEventStart, nil)
		s.True(changed)
		s.NoError(err)
	}
	s.Len(qr.GetBuffered(), 5)
	s.Len(qr.GetStarted(), 5)

	completeQuery := func(q Query) {
		if q.State() == QueryStateBuffered {
			changed, err := q.RecordEvent(QueryEventStart, nil)
			s.True(changed)
			s.NoError(err)
		}
		changed, err := q.RecordEvent(QueryEventPersistenceConditionSatisfied, nil)
		s.False(changed)
		s.NoError(err)
		changed, err = q.RecordEvent(QueryEventRecordResult, &shared.WorkflowQueryResult{})
		s.True(changed)
		s.NoError(err)
		s.Equal(QueryStateCompleted, q.State())
	}

	expireQuery := func(q Query) {
		changed, err := q.RecordEvent(QueryEventExpire, nil)
		s.True(changed)
		s.NoError(err)
	}

	completeQuery(queries[0])
	expireQuery(queries[1])
	completeQuery(queries[8])
	expireQuery(queries[9])

	s.Len(qr.GetBuffered(), 3)
	s.Len(qr.GetStarted(), 3)
}

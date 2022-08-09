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

package queues

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	convertSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestConvertSuite(t *testing.T) {
	s := new(convertSuite)
	suite.Run(t, s)
}

func (s *convertSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *convertSuite) TestConvertPredicate_All() {
	predicate := predicates.Universal[tasks.Task]()
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
}

func (s *convertSuite) TestConvertPredicate_Empty() {
	predicate := predicates.Empty[tasks.Task]()
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
}

func (s *convertSuite) TestConvertPredicate_And() {
	predicates := []tasks.Predicate{
		predicates.And(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.And(
			predicates.Or[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
			predicates.Or[tasks.Task](
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
	}

	for _, predicate := range predicates {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func (s *convertSuite) TestConvertPredicate_Or() {
	predicates := []tasks.Predicate{
		predicates.Or(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.Or(
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
			predicates.And[tasks.Task](
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewNamespacePredicate([]string{uuid.New()}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.New()}),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
				}),
			),
		),
	}

	for _, predicate := range predicates {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func (s *convertSuite) TestConvertPredicate_Not() {
	predicates := []tasks.Predicate{
		predicates.Not(predicates.Universal[tasks.Task]()),
		predicates.Not(predicates.Empty[tasks.Task]()),
		predicates.Not(predicates.And[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.New()}),
			tasks.NewTypePredicate([]enumsspb.TaskType{}),
		)),
		predicates.Not(predicates.Or[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.New()}),
			tasks.NewTypePredicate([]enumsspb.TaskType{}),
		)),
		predicates.Not(predicates.Not(predicates.Empty[tasks.Task]())),
		predicates.Not[tasks.Task](tasks.NewNamespacePredicate([]string{uuid.New()})),
		predicates.Not[tasks.Task](tasks.NewTypePredicate([]enumsspb.TaskType{
			enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		})),
	}

	for _, predicate := range predicates {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func (s *convertSuite) TestConvertPredicate_NamespaceID() {
	predicates := []tasks.Predicate{
		tasks.NewNamespacePredicate(nil),
		tasks.NewNamespacePredicate([]string{}),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New()}),
	}

	for _, predicate := range predicates {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func (s *convertSuite) TestConvertPredicate_TaskType() {
	predicates := []tasks.Predicate{
		tasks.NewTypePredicate(nil),
		tasks.NewTypePredicate([]enumsspb.TaskType{}),
		tasks.NewTypePredicate([]enumsspb.TaskType{
			enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
			enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		}),
	}

	for _, predicate := range predicates {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate)))
	}
}

func (s *convertSuite) TestConvertTaskKey() {
	key := NewRandomKey()
	s.Equal(key, FromPersistenceTaskKey(
		ToPersistenceTaskKey(key),
	))
}

func (s *convertSuite) TestConvertTaskRange() {
	r := NewRandomRange()
	s.Equal(r, FromPersistenceRange(
		ToPersistenceRange(r),
	))
}

func (s *convertSuite) TestConvertScope() {
	scope := NewScope(
		NewRandomRange(),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
	)

	s.Equal(scope, FromPersistenceScope(
		ToPersistenceScope(scope),
	))
}

func (s *convertSuite) TestConvertQueueState() {
	readerScopes := map[int32][]Scope{
		0: {},
		1: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
			),
		},
		123: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New()}),
			),
			NewScope(
				NewRandomRange(),
				tasks.NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
					enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
				}),
			),
		},
	}

	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), 0),
	}

	s.Equal(queueState, FromPersistenceQueueState(
		ToPersistenceQueueState(queueState),
	))
}

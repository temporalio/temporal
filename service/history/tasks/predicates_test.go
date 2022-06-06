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

package tasks

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"golang.org/x/exp/slices"
)

type (
	predicatesSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestPredicateSuite(t *testing.T) {
	s := new(predicatesSuite)
	suite.Run(t, s)
}

func (s *predicatesSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *predicatesSuite) TestNamespacePredicate() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)
	for _, id := range namespaceIDs {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(id).Times(1)
		s.True(p.Test(mockTask))
	}

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(1)
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestTypePredicate() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)
	for _, taskType := range types {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(taskType).Times(1)
		s.True(p.Test(mockTask))
	}

	for _, taskType := range enumsspb.TaskType_value {
		if slices.Index(types, enumsspb.TaskType(taskType)) != -1 {
			continue
		}

		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(taskType)).Times(1)
		s.False(p.Test(mockTask))
	}
}

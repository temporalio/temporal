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

package serialization

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/definition"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/shuffle"
)

type (
	taskSerializerSuite struct {
		suite.Suite
		*require.Assertions

		namespaceID    string
		workflowID     string
		runID          string
		taskSerializer *TaskSerializer
	}
)

func TestTaskSerializerSuite(t *testing.T) {
	suite.Run(t, new(taskSerializerSuite))
}

func (s *taskSerializerSuite) SetupSuite() {

}

func (s *taskSerializerSuite) TearDownSuite() {

}

func (s *taskSerializerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespaceID = "random namespace ID"
	s.workflowID = "random workflow ID"
	s.runID = "random run ID"
	s.taskSerializer = NewTaskSerializer(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
}

func (s *taskSerializerSuite) TearDownTest() {

}

func (s *taskSerializerSuite) TestActivityTask() {
	activityTask := &definition.ActivityTask{
		VisibilityTimestamp: time.Unix(0, rand.Int63()),
		TaskID:              rand.Int63(),
		NamespaceID:         uuid.New().String(),
		TaskQueue:           shuffle.String("random task queue name"),
		ScheduleID:          rand.Int63(),
		Version:             rand.Int63(),
	}

	blobs, err := s.taskSerializer.SerializeTransferTasks([]p.Task{activityTask})
	s.NoError(err)
	tasks, err := s.taskSerializer.DeserializeTransferTasks(blobs)
	s.NoError(err)
	s.Equal([]p.Task{activityTask}, tasks)
}

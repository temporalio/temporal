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

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	processBufferSuite struct {
		suite.Suite
	}

	job struct {
		id     int
		policy enumspb.ScheduleOverlapPolicy
	}
)

func (j *job) GetOverlapPolicy() enumspb.ScheduleOverlapPolicy { return j.policy }

func jobIds(jobs []*job) (out []int) {
	for _, j := range jobs {
		out = append(out, j.id)
	}
	return
}

func identity[T any](v T) T { return v }

func TestProcessBuffer(t *testing.T) {
	suite.Run(t, new(processBufferSuite))
}

func (s *processBufferSuite) TestProcessBufferEmpty() {
	buffer := []*job{}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessSkipRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}}
	action := processBuffer(buffer, true, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessSkipNotRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Equal(3, action.nonOverlappingStart.id)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessBufferOneRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}
	action := processBuffer(buffer, true, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Equal([]int{3}, jobIds(action.newBuffer))
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessBufferOneNotRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Equal(3, action.nonOverlappingStart.id)
	s.Equal([]int{5}, jobIds(action.newBuffer))
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessBufferAllRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}}
	action := processBuffer(buffer, true, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Equal([]int{3, 5, 7}, jobIds(action.newBuffer))
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessBufferAllNotRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Equal(3, action.nonOverlappingStart.id)
	s.Equal([]int{5, 7}, jobIds(action.newBuffer))
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessCancelRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}}
	action := processBuffer(buffer, true, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Equal([]int{3, 5, 7}, jobIds(action.newBuffer))
	s.True(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessCancelNotRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	// optimization: 3 and 5 don't even get started since they would be immediately cancelled
	s.Equal(7, action.nonOverlappingStart.id)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessTerminateRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}}
	action := processBuffer(buffer, true, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	s.Nil(action.nonOverlappingStart)
	s.Equal([]int{3, 5, 7}, jobIds(action.newBuffer))
	s.False(action.needCancel)
	s.True(action.needTerminate)
}

func (s *processBufferSuite) TestProcessTerminateNotRunning() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Empty(action.overlappingStarts)
	// optimization: 3 and 5 don't even get started since they would be immediately terminated
	s.Equal(7, action.nonOverlappingStart.id)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessAllowAll() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}
	action := processBuffer(buffer, false, identity[enumspb.ScheduleOverlapPolicy])
	s.Equal([]int{3, 5, 7}, jobIds(action.overlappingStarts))
	s.Nil(action.nonOverlappingStart)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

func (s *processBufferSuite) TestProcessWithResolve() {
	buffer := []*job{{3, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED}, {5, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED}, {7, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED}}
	terminate := func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
		return enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	}
	action := processBuffer(buffer, false, terminate)
	s.Empty(action.overlappingStarts)
	s.Equal(7, action.nonOverlappingStart.id)
	s.Empty(action.newBuffer)
	s.False(action.needCancel)
	s.False(action.needTerminate)
}

// TODO: add test cases for mixed policies

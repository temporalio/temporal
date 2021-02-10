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

package executions

import (
	"testing"

	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	activityIDValidatorTestSuite struct {
		suite.Suite
	}
)

func TestActivityIdValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(activityIDValidatorTestSuite))
}

func (s *activityIDValidatorTestSuite) TestAIVTReturnsNoCorruptionWhenNoActivitiesPresent() {
	testData := executionData{mutableState: &persistencespb.WorkflowMutableState{}}
	result := newActivityIDValidator().validate(&testData)
	s.True(result.isValid)
}

func (s *activityIDValidatorTestSuite) TestAIVTReturnsNoCorruptionWhenActivityIdsAreValid() {
	testData := executionData{}
	testData.mutableState = &persistencespb.WorkflowMutableState{
		NextEventId: 5, ActivityInfos: map[int64]*persistencespb.ActivityInfo{1: {}, 2: {}}}
	result := newActivityIDValidator().validate(&testData)
	s.True(result.isValid)
}

func (s *activityIDValidatorTestSuite) TestAIVTReturnsFailureInfoOnCorruptActivityId() {
	testData := executionData{}
	testData.mutableState = &persistencespb.WorkflowMutableState{
		NextEventId: 5, ActivityInfos: map[int64]*persistencespb.ActivityInfo{1: {}, 6: {}, 2: {}}}
	result := newActivityIDValidator().validate(&testData)
	s.False(result.isValid)
}

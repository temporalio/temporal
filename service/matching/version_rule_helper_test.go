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

package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
	taskqueue2 "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/persistence/v1"
)

func TestFindAssignmentBuildId_NoRules(t *testing.T) {
	assert.Equal(t, "", FindAssignmentBuildId(nil))
}

func TestFindAssignmentBuildId_OneFullRule(t *testing.T) {
	buildId := "bld"
	assert.Equal(t, buildId, FindAssignmentBuildId([]*persistence.AssignmentRule{createFullAssignmentRule(buildId)}))
}

func TestFindAssignmentBuildId_TwoFullRules(t *testing.T) {
	buildId := "bld"
	buildId2 := "bld2"
	assert.Equal(t, buildId, FindAssignmentBuildId([]*persistence.AssignmentRule{createFullAssignmentRule(buildId), createFullAssignmentRule(buildId2)}))
}

func createFullAssignmentRule(buildId string) *persistence.AssignmentRule {
	return &persistence.AssignmentRule{Rule: &taskqueue2.BuildIdAssignmentRule{TargetBuildId: buildId}}
}

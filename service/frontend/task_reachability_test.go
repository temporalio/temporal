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

package frontend

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRedirectRuleSource(t *testing.T) {
	t.Parallel()
}

func TestGetUpstreamBuildIds(t *testing.T) {
	t.Parallel()
}

func TestExistsBackloggedActivityOrWFAssignedTo(t *testing.T) {
	t.Parallel()
}

func TestIsReachableAssignmentRuleTarget(t *testing.T) {
	t.Parallel()
}

func TestExistsWFAssignedToAny(t *testing.T) {
	t.Parallel()
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	tq := "test-query-tq"

	queryOpen := makeBuildIdQuery(buildIdsOfInterest, tq, true)
	fmt.Printf("%s\n", queryOpen)
	expectedQueryOpen := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2',unversioned)) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQueryOpen, queryOpen)

	queryClosed := makeBuildIdQuery(buildIdsOfInterest, tq, false)
	fmt.Printf("%s\n", queryClosed)
	expectedQueryClosed := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2',unversioned)) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQueryClosed, queryClosed)
}

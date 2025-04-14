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

package api

import (
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestResolveDuplicateWorkflowStart(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := timeSource.Now()

	testCases := []struct {
		gracePeriod          time.Duration
		currentWorkflowStart time.Time
		expectError          bool
	}{
		{
			gracePeriod:          time.Duration(0 * time.Second),
			currentWorkflowStart: now,
			expectError:          false,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now,
			expectError:          true,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now.Add(-2 * time.Second),
			expectError:          false,
		},
	}

	config := tests.NewDynamicConfig()

	mockShard := shard.NewTestContextWithTimeSource(
		gomock.NewController(t),
		&persistencespb.ShardInfo{RangeId: 1},
		config,
		timeSource,
	)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		"target_cluster",
	)

	for _, tc := range testCases {
		config.WorkflowIdReuseMinimalInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(tc.gracePeriod)
		workflowKey := definition.WorkflowKey{
			NamespaceID: uuid.NewUUID().String(),
			WorkflowID:  "workflowID",
			RunID:       "oldRunID",
		}
		_, err := resolveDuplicateWorkflowStart(mockShard, tc.currentWorkflowStart, workflowKey, namespaceEntry, "newRunID", nil, false)

		if tc.expectError {
			assert.Error(t, err)
			var resourceErr *serviceerror.ResourceExhausted
			assert.ErrorAs(t, err, &resourceErr)

		} else {
			assert.NoError(t, err)
		}
	}
}

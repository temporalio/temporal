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

package persistencetest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetQueueKey_Default(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t)
	assert.Equal(t, persistence.QueueTypeHistoryNormal, queueKey.QueueType)
	assert.Equal(t, tasks.CategoryTransfer, queueKey.Category)
	assert.Equal(t, "test-source-cluster-TestGetQueueKey_Default", queueKey.SourceCluster)
	assert.Equal(t, "test-target-cluster-TestGetQueueKey_Default", queueKey.TargetCluster)
}

func TestGetQueueKey_WithOptions(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t,
		persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ),
		persistencetest.WithCategory(tasks.CategoryTimer),
	)
	assert.Equal(t, persistence.QueueTypeHistoryDLQ, queueKey.QueueType)
	assert.Equal(t, tasks.CategoryTimer, queueKey.Category)
	assert.Equal(t, "test-source-cluster-TestGetQueueKey_WithOptions", queueKey.SourceCluster)
	assert.Equal(t, "test-target-cluster-TestGetQueueKey_WithOptions", queueKey.TargetCluster)
}

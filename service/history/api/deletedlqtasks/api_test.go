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

package deletedlqtasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/service/history/api/deletedlqtasks"
	"go.temporal.io/server/service/history/tasks"
)

func TestInvoke_InvalidCategory(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
	_, err := deletedlqtasks.Invoke(context.Background(), nil, &historyservice.DeleteDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  -1,
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
		},
	}, tasks.NewDefaultTaskCategoryRegistry())
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	assert.ErrorContains(t, err, "-1")
}

func TestInvoke_ErrDeleteMissingMessageIDUpperBound(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
	_, err := deletedlqtasks.Invoke(context.Background(), nil, &historyservice.DeleteDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  int32(queueKey.Category.ID()),
			SourceCluster: queueKey.SourceCluster,
			TargetCluster: queueKey.TargetCluster,
		},
	}, tasks.NewDefaultTaskCategoryRegistry())
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	assert.ErrorContains(t, err, "inclusive_max_task_metadata")
}

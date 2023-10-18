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

package getdlqtasks_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

// failingHistoryTaskQueueManager is a [persistence.HistoryTaskQueueManager] that always fails.
type failingHistoryTaskQueueManager struct {
	persistence.HistoryTaskQueueManager
}

func TestInvoke_InvalidTaskCategory(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(context.Background(),
		nil,
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: -1,
			},
		})

	var invalidArgErr *serviceerror.InvalidArgument

	require.ErrorAs(t, err, &invalidArgErr)
	assert.ErrorContains(t, invalidArgErr, "Invalid task category")
	assert.ErrorContains(t, invalidArgErr, "-1")
}

func TestInvoke_ZeroPageSize(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(
		context.Background(),
		new(persistence.HistoryTaskQueueManagerImpl),
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: int32(tasks.CategoryTransfer.ID()),
			},
			PageSize: 0,
		},
	)

	assert.ErrorIs(t, err, consts.ErrInvalidPageSize)
}

func TestInvoke_UnavailableError(t *testing.T) {
	t.Parallel()

	_, err := getdlqtasks.Invoke(
		context.Background(),
		failingHistoryTaskQueueManager{},
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory: int32(tasks.CategoryTransfer.ID()),
			},
			PageSize: 0,
		},
	)

	var unavailableErr *serviceerror.Unavailable

	require.ErrorAs(t, err, &unavailableErr)
	assert.ErrorContains(t, unavailableErr, "some random error")
}

func (m failingHistoryTaskQueueManager) ReadRawTasks(
	context.Context,
	*persistence.ReadRawTasksRequest,
) (*persistence.ReadRawTasksResponse, error) {
	return nil, errors.New("some random error")
}

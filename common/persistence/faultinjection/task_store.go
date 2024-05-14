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

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

type (
	faultInjectionTaskStore struct {
		baseTaskStore persistence.TaskStore
		generator     faultGenerator
	}
)

func newFaultInjectionTaskStore(
	baseStore persistence.TaskStore,
	generator faultGenerator,
) *faultInjectionTaskStore {
	return &faultInjectionTaskStore{
		baseTaskStore: baseStore,
		generator:     generator,
	}
}

func (t *faultInjectionTaskStore) Close() {
	t.baseTaskStore.Close()
}

func (t *faultInjectionTaskStore) GetName() string {
	return t.baseTaskStore.GetName()
}

func (t *faultInjectionTaskStore) CreateTaskQueue(
	ctx context.Context,
	request *persistence.InternalCreateTaskQueueRequest,
) error {
	return inject0(t.generator.generate(), func() error {
		return t.baseTaskStore.CreateTaskQueue(ctx, request)
	})
}

func (t *faultInjectionTaskStore) GetTaskQueue(
	ctx context.Context,
	request *persistence.InternalGetTaskQueueRequest,
) (*persistence.InternalGetTaskQueueResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.InternalGetTaskQueueResponse, error) {
		return t.baseTaskStore.GetTaskQueue(ctx, request)
	})
}

func (t *faultInjectionTaskStore) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.UpdateTaskQueueResponse, error) {
		return t.baseTaskStore.UpdateTaskQueue(ctx, request)
	})
}

func (t *faultInjectionTaskStore) ListTaskQueue(
	ctx context.Context,
	request *persistence.ListTaskQueueRequest,
) (*persistence.InternalListTaskQueueResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.InternalListTaskQueueResponse, error) {
		return t.baseTaskStore.ListTaskQueue(ctx, request)
	})
}

func (t *faultInjectionTaskStore) DeleteTaskQueue(
	ctx context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	return inject0(t.generator.generate(), func() error {
		return t.baseTaskStore.DeleteTaskQueue(ctx, request)
	})
}

func (t *faultInjectionTaskStore) CreateTasks(
	ctx context.Context,
	request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.CreateTasksResponse, error) {
		return t.baseTaskStore.CreateTasks(ctx, request)
	})
}

func (t *faultInjectionTaskStore) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.InternalGetTasksResponse, error) {
		return t.baseTaskStore.GetTasks(ctx, request)
	})
}

func (t *faultInjectionTaskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	return inject1(t.generator.generate(), func() (int, error) {
		return t.baseTaskStore.CompleteTasksLessThan(ctx, request)
	})
}

func (t *faultInjectionTaskStore) GetTaskQueueUserData(ctx context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.InternalGetTaskQueueUserDataResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.InternalGetTaskQueueUserDataResponse, error) {
		return t.baseTaskStore.GetTaskQueueUserData(ctx, request)
	})
}

func (t *faultInjectionTaskStore) UpdateTaskQueueUserData(ctx context.Context, request *persistence.InternalUpdateTaskQueueUserDataRequest) error {
	return inject0(t.generator.generate(), func() error {
		return t.baseTaskStore.UpdateTaskQueueUserData(ctx, request)
	})
}

func (t *faultInjectionTaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
	return inject1(t.generator.generate(), func() (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
		return t.baseTaskStore.ListTaskQueueUserDataEntries(ctx, request)
	})
}

func (t *faultInjectionTaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *persistence.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	return inject1(t.generator.generate(), func() ([]string, error) {
		return t.baseTaskStore.GetTaskQueuesByBuildId(ctx, request)
	})
}

func (t *faultInjectionTaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *persistence.CountTaskQueuesByBuildIdRequest) (int, error) {
	return inject1(t.generator.generate(), func() (int, error) {
		return t.baseTaskStore.CountTaskQueuesByBuildId(ctx, request)
	})
}

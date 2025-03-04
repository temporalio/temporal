// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

// Code generated by gowrap. DO NOT EDIT.
// template: gowrap_template
// gowrap: http://github.com/hexdigest/gowrap

package faultinjection

//go:generate gowrap gen -p go.temporal.io/server/common/persistence -i TaskStore -t gowrap_template -o task_store_gen.go -l ""

import (
	"context"

	_sourcePersistence "go.temporal.io/server/common/persistence"
)

type (
	// faultInjectionTaskStore implements TaskStore interface with fault injection.
	faultInjectionTaskStore struct {
		_sourcePersistence.TaskStore
		generator faultGenerator
	}
)

// newFaultInjectionTaskStore returns faultInjectionTaskStore.
func newFaultInjectionTaskStore(
	baseStore _sourcePersistence.TaskStore,
	generator faultGenerator,
) *faultInjectionTaskStore {
	return &faultInjectionTaskStore{
		TaskStore: baseStore,
		generator: generator,
	}
}

// CompleteTasksLessThan wraps TaskStore.CompleteTasksLessThan.
func (d faultInjectionTaskStore) CompleteTasksLessThan(ctx context.Context, request *_sourcePersistence.CompleteTasksLessThanRequest) (i1 int, err error) {
	err = d.generator.generate("CompleteTasksLessThan").inject(func() error {
		i1, err = d.TaskStore.CompleteTasksLessThan(ctx, request)
		return err
	})
	return
}

// CountTaskQueuesByBuildId wraps TaskStore.CountTaskQueuesByBuildId.
func (d faultInjectionTaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *_sourcePersistence.CountTaskQueuesByBuildIdRequest) (i1 int, err error) {
	err = d.generator.generate("CountTaskQueuesByBuildId").inject(func() error {
		i1, err = d.TaskStore.CountTaskQueuesByBuildId(ctx, request)
		return err
	})
	return
}

// CreateTaskQueue wraps TaskStore.CreateTaskQueue.
func (d faultInjectionTaskStore) CreateTaskQueue(ctx context.Context, request *_sourcePersistence.InternalCreateTaskQueueRequest) (err error) {
	err = d.generator.generate("CreateTaskQueue").inject(func() error {
		err = d.TaskStore.CreateTaskQueue(ctx, request)
		return err
	})
	return
}

// CreateTasks wraps TaskStore.CreateTasks.
func (d faultInjectionTaskStore) CreateTasks(ctx context.Context, request *_sourcePersistence.InternalCreateTasksRequest) (cp1 *_sourcePersistence.CreateTasksResponse, err error) {
	err = d.generator.generate("CreateTasks").inject(func() error {
		cp1, err = d.TaskStore.CreateTasks(ctx, request)
		return err
	})
	return
}

// DeleteTaskQueue wraps TaskStore.DeleteTaskQueue.
func (d faultInjectionTaskStore) DeleteTaskQueue(ctx context.Context, request *_sourcePersistence.DeleteTaskQueueRequest) (err error) {
	err = d.generator.generate("DeleteTaskQueue").inject(func() error {
		err = d.TaskStore.DeleteTaskQueue(ctx, request)
		return err
	})
	return
}

// GetTaskQueue wraps TaskStore.GetTaskQueue.
func (d faultInjectionTaskStore) GetTaskQueue(ctx context.Context, request *_sourcePersistence.InternalGetTaskQueueRequest) (ip1 *_sourcePersistence.InternalGetTaskQueueResponse, err error) {
	err = d.generator.generate("GetTaskQueue").inject(func() error {
		ip1, err = d.TaskStore.GetTaskQueue(ctx, request)
		return err
	})
	return
}

// GetTaskQueueUserData wraps TaskStore.GetTaskQueueUserData.
func (d faultInjectionTaskStore) GetTaskQueueUserData(ctx context.Context, request *_sourcePersistence.GetTaskQueueUserDataRequest) (ip1 *_sourcePersistence.InternalGetTaskQueueUserDataResponse, err error) {
	err = d.generator.generate("GetTaskQueueUserData").inject(func() error {
		ip1, err = d.TaskStore.GetTaskQueueUserData(ctx, request)
		return err
	})
	return
}

// GetTaskQueuesByBuildId wraps TaskStore.GetTaskQueuesByBuildId.
func (d faultInjectionTaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *_sourcePersistence.GetTaskQueuesByBuildIdRequest) (sa1 []string, err error) {
	err = d.generator.generate("GetTaskQueuesByBuildId").inject(func() error {
		sa1, err = d.TaskStore.GetTaskQueuesByBuildId(ctx, request)
		return err
	})
	return
}

// GetTasks wraps TaskStore.GetTasks.
func (d faultInjectionTaskStore) GetTasks(ctx context.Context, request *_sourcePersistence.GetTasksRequest) (ip1 *_sourcePersistence.InternalGetTasksResponse, err error) {
	err = d.generator.generate("GetTasks").inject(func() error {
		ip1, err = d.TaskStore.GetTasks(ctx, request)
		return err
	})
	return
}

// ListTaskQueue wraps TaskStore.ListTaskQueue.
func (d faultInjectionTaskStore) ListTaskQueue(ctx context.Context, request *_sourcePersistence.ListTaskQueueRequest) (ip1 *_sourcePersistence.InternalListTaskQueueResponse, err error) {
	err = d.generator.generate("ListTaskQueue").inject(func() error {
		ip1, err = d.TaskStore.ListTaskQueue(ctx, request)
		return err
	})
	return
}

// ListTaskQueueUserDataEntries wraps TaskStore.ListTaskQueueUserDataEntries.
func (d faultInjectionTaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *_sourcePersistence.ListTaskQueueUserDataEntriesRequest) (ip1 *_sourcePersistence.InternalListTaskQueueUserDataEntriesResponse, err error) {
	err = d.generator.generate("ListTaskQueueUserDataEntries").inject(func() error {
		ip1, err = d.TaskStore.ListTaskQueueUserDataEntries(ctx, request)
		return err
	})
	return
}

// UpdateTaskQueue wraps TaskStore.UpdateTaskQueue.
func (d faultInjectionTaskStore) UpdateTaskQueue(ctx context.Context, request *_sourcePersistence.InternalUpdateTaskQueueRequest) (up1 *_sourcePersistence.UpdateTaskQueueResponse, err error) {
	err = d.generator.generate("UpdateTaskQueue").inject(func() error {
		up1, err = d.TaskStore.UpdateTaskQueue(ctx, request)
		return err
	})
	return
}

// UpdateTaskQueueUserData wraps TaskStore.UpdateTaskQueueUserData.
func (d faultInjectionTaskStore) UpdateTaskQueueUserData(ctx context.Context, request *_sourcePersistence.InternalUpdateTaskQueueUserDataRequest) (err error) {
	err = d.generator.generate("UpdateTaskQueueUserData").inject(func() error {
		err = d.TaskStore.UpdateTaskQueueUserData(ctx, request)
		return err
	})
	return
}

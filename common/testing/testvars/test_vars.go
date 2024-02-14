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

package testvars

import (
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	TestVars struct {
		testName string
		testHash uint32
		an       Any
		kv       sync.Map
	}
)

func New(testName string) *TestVars {
	th := hash(testName)
	return &TestVars{
		testName: testName,
		testHash: th,
		an:       newAny(testName, th),
	}
}

func (tv *TestVars) key(typ string, key []string) string {
	if len(key) == 0 {
		return typ
	}
	return typ + "_" + strings.Join(key, "_")
}

func (tv *TestVars) getOrCreate(typ string, key []string, initialVal ...any) any {
	kvKey := tv.key(typ, key)
	var kvVal any
	if len(initialVal) == 0 {
		kvVal = tv.testName + "_" + kvKey
	} else {
		kvVal = initialVal[0]
	}
	v, _ := tv.kv.LoadOrStore(kvKey, kvVal)
	return v
}

func (tv *TestVars) set(typ string, key []string, val any) {
	kvKey := tv.key(typ, key)
	tv.kv.Store(kvKey, val)
}

func (tv *TestVars) clone() *TestVars {
	tv2 := New(tv.testName)
	tv.kv.Range(func(key, value any) bool {
		tv2.kv.Store(key, value)
		return true
	})

	return tv2
}

func (tv *TestVars) cloneSet(typ string, key []string, val any) *TestVars {
	newTv := tv.clone()
	newTv.set(typ, key, val)
	return newTv
}

// ----------- Methods for every "type" ------------
// TODO: add more as you need them.

func (tv *TestVars) WorkflowID(key ...string) string {
	return tv.getOrCreate("workflow_id", key).(string)
}

func (tv *TestVars) WithWorkflowID(workflowID string, key ...string) *TestVars {
	return tv.cloneSet("workflow_id", key, workflowID)
}

func (tv *TestVars) RunID(key ...string) string {
	return tv.getOrCreate("run_id", key, uuid.New()).(string)
}

func (tv *TestVars) WithRunID(runID string, key ...string) *TestVars {
	return tv.cloneSet("run_id", key, runID)
}

func (tv *TestVars) WorkflowExecution(key ...string) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(key...),
		RunId:      tv.RunID(key...),
	}
}

func (tv *TestVars) TaskQueue(key ...string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name: tv.getOrCreate("task_queue", key).(string),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
}

func (tv *TestVars) WithTaskQueue(taskQueue string, key ...string) *TestVars {
	return tv.cloneSet("task_queue", key, taskQueue)
}

func (tv *TestVars) StickyTaskQueue(key ...string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name:       tv.getOrCreate("sticky_task_queue", key).(string),
		Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
		NormalName: tv.getOrCreate("task_queue", key).(string),
	}
}

func (tv *TestVars) WithStickyTaskQueue(stickyTaskQueue string, key ...string) *TestVars {
	return tv.cloneSet("sticky_task_queue", key, stickyTaskQueue)
}

func (tv *TestVars) WorkflowType(key ...string) *commonpb.WorkflowType {
	return &commonpb.WorkflowType{
		Name: tv.getOrCreate("workflow_type", key).(string),
	}
}

func (tv *TestVars) WithWorkflowType(workflowType string, key ...string) *TestVars {
	return tv.cloneSet("workflow_type", key, workflowType)
}

func (tv *TestVars) ActivityID(key ...string) string {
	return tv.getOrCreate("activity_id", key).(string)
}

func (tv *TestVars) WithActivityID(activityID string, key ...string) *TestVars {
	return tv.cloneSet("activity_id", key, activityID)
}

func (tv *TestVars) ActivityType(key ...string) *commonpb.ActivityType {
	return &commonpb.ActivityType{
		Name: tv.getOrCreate("activity_type", key).(string),
	}
}

func (tv *TestVars) WithActivityType(workflowType string, key ...string) *TestVars {
	return tv.cloneSet("activity_type", key, workflowType)
}

func (tv *TestVars) MessageID(key ...string) string {
	return tv.getOrCreate("message_id", key).(string)
}

func (tv *TestVars) WithMessageID(messageID string, key ...string) *TestVars {
	return tv.cloneSet("message_id", key, messageID)
}

func (tv *TestVars) UpdateID(key ...string) string {
	return tv.getOrCreate("update_id", key).(string)
}

func (tv *TestVars) WithUpdateID(updateID string, key ...string) *TestVars {
	return tv.cloneSet("update_id", key, updateID)
}

func (tv *TestVars) HandlerName(key ...string) string {
	return tv.getOrCreate("handler_name", key).(string)
}

func (tv *TestVars) WithHandlerName(handlerName string, key ...string) *TestVars {
	return tv.cloneSet("handler_name", key, handlerName)
}

func (tv *TestVars) WorkerIdentity(key ...string) string {
	return tv.getOrCreate("worker_identity", key).(string)
}

func (tv *TestVars) WithWorkerIdentity(identity string, key ...string) *TestVars {
	return tv.cloneSet("worker_identity", key, identity)
}

// ----------- Generic methods ------------

func (tv *TestVars) InfiniteTimeout() *durationpb.Duration {
	t := 10 * time.Hour
	return durationpb.New(t)
}

func (tv *TestVars) Any() Any {
	return tv.an
}

func (tv *TestVars) String(key ...string) string {
	return tv.getOrCreate("string", key).(string)
}

func (tv *TestVars) WithString(str string, key ...string) *TestVars {
	return tv.cloneSet("string", key, str)
}

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
	"math/rand"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

type (
	TestVars struct {
		testName string
		kv       sync.Map
	}
)

func New(testName string) *TestVars {
	return &TestVars{
		testName: testName,
	}
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (tv *TestVars) key(typ string, key []string) string {
	if len(key) == 0 {
		return typ
	}
	return typ + "_" + strings.Join(key, "_")
}

func (tv *TestVars) getOrCreate(typ string, key []string) string {
	keyStr := tv.key(typ, key)
	v, _ := tv.kv.LoadOrStore(keyStr, tv.testName+"_"+keyStr)
	return v.(string)
}

func (tv *TestVars) set(typ, val string, key []string) {
	keyStr := tv.key(typ, key)
	tv.kv.Store(keyStr, val)
}

func (tv *TestVars) clone() *TestVars {
	tv2 := New(tv.testName)
	tv.kv.Range(func(key, value any) bool {
		tv2.kv.Store(key, value)
		return true
	})

	return tv2
}

func (tv *TestVars) cloneSet(typ, val string, key []string) *TestVars {
	newTv := tv.clone()
	newTv.set(typ, val, key)
	return newTv
}

// ----------- Methods for every "type" ------------
// TODO: add more as you need them.

func (tv *TestVars) WorkflowID(key ...string) string {
	return tv.getOrCreate("workflow_id", key)
}

func (tv *TestVars) WithWorkflowID(workflowID string, key ...string) *TestVars {
	return tv.cloneSet("workflow_id", workflowID, key)
}

func (tv *TestVars) RunID(key ...string) string {
	return tv.getOrCreate("run_id", key)
}

func (tv *TestVars) WithRunID(runID string, key ...string) *TestVars {
	return tv.cloneSet("run_id", runID, key)
}

func (tv *TestVars) WorkflowExecution(key ...string) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(key...),
		RunId:      tv.RunID(key...),
	}
}

func (tv *TestVars) TaskQueue(key ...string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name: tv.getOrCreate("task_queue", key),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
}

func (tv *TestVars) WithTaskQueue(taskQueue string, key ...string) *TestVars {
	return tv.cloneSet("task_queue", taskQueue, key)
}

func (tv *TestVars) StickyTaskQueue(key ...string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name: tv.getOrCreate("sticky_task_queue", key),
		Kind: enumspb.TASK_QUEUE_KIND_STICKY,
	}
}

func (tv *TestVars) WithStickyTaskQueue(stickyTaskQueue string, key ...string) *TestVars {
	return tv.cloneSet("sticky_task_queue", stickyTaskQueue, key)
}

func (tv *TestVars) WorkflowType(key ...string) *commonpb.WorkflowType {
	return &commonpb.WorkflowType{
		Name: tv.getOrCreate("workflow_type", key),
	}
}

func (tv *TestVars) WithWorkflowType(workflowType string, key ...string) *TestVars {
	return tv.cloneSet("workflow_type", workflowType, key)
}

func (tv *TestVars) ActivityID(key ...string) string {
	return tv.getOrCreate("activity_id", key)
}

func (tv *TestVars) WithActivityID(activityID string, key ...string) *TestVars {
	return tv.cloneSet("activity_id", activityID, key)
}

func (tv *TestVars) ActivityType(key ...string) *commonpb.ActivityType {
	return &commonpb.ActivityType{
		Name: tv.getOrCreate("activity_type", key),
	}
}

func (tv *TestVars) WithActivityType(workflowType string, key ...string) *TestVars {
	return tv.cloneSet("activity_type", workflowType, key)
}

func (tv *TestVars) MessageID(key ...string) string {
	return tv.getOrCreate("message_id", key)
}

func (tv *TestVars) WithMessageID(messageID string, key ...string) *TestVars {
	return tv.cloneSet("message_id", messageID, key)
}

func (tv *TestVars) UpdateID(key ...string) string {
	return tv.getOrCreate("update_id", key)
}

func (tv *TestVars) WithUpdateID(updateID string, key ...string) *TestVars {
	return tv.cloneSet("update_id", updateID, key)
}

func (tv *TestVars) HandlerName(key ...string) string {
	return tv.getOrCreate("handler_name", key)
}

func (tv *TestVars) WithHandlerName(handlerName string, key ...string) *TestVars {
	return tv.cloneSet("handler_name", handlerName, key)
}

func (tv *TestVars) WorkerIdentity(key ...string) string {
	return tv.getOrCreate("worker_identity", key)
}

func (tv *TestVars) WithWorkerIdentity(identity string, key ...string) *TestVars {
	return tv.cloneSet("worker_identity", identity, key)
}

// ----------- Generic methods ------------

func (tv *TestVars) InfiniteTimeout() *time.Duration {
	t := 10 * time.Hour
	return &t
}

func (tv *TestVars) Any() string {
	return tv.testName + "_any_random_string_" + randString(5)
}

func (tv *TestVars) String(key ...string) string {
	return tv.getOrCreate("string", key)
}

func (tv *TestVars) WithString(str string, key ...string) *TestVars {
	return tv.cloneSet("string", str, key)
}

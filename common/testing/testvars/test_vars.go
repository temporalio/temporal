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
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	TestVars struct {
		testName string
		testHash uint32
		an       Any
		values   sync.Map
		ns       sync.Map
	}
	testNamer interface {
		Name() string
	}
)

func New(testNamer testNamer) *TestVars {
	return newFromName(testNamer.Name())
}

func newFromName(testName string) *TestVars {
	th := hash(testName)
	return &TestVars{
		testName: testName,
		testHash: th,
		an:       newAny(testName, th),
	}
}

func getOrCreate[T any](tv *TestVars, key string, initialValGen func(key string) T, valNSetter func(val T, n int) T) T {
	v, _ := tv.values.LoadOrStore(key, initialValGen(key))

	n, ok := tv.ns.Load(key)
	if !ok {
		//revive:disable-next-line:unchecked-type-assertion
		return v.(T)
	}

	//revive:disable-next-line:unchecked-type-assertion
	return valNSetter(v.(T), n.(int))
}

func (tv *TestVars) stringNSetter(v string, n int) string {
	return fmt.Sprintf("%s_%d", v, n)
}

// Generate new uuid for every new n.
func (tv *TestVars) uuidNSetter(_ string, _ int) string {
	return tv.uuidString("")
}

func (tv *TestVars) intNSetter(v int, n int) int {
	return n*1000 + v
}

// Use this setter for entities that don't support setting n.
func unsupportedNSetter[T any](v T, _ int) T {
	panic(fmt.Sprintf("setting n on type %T is not supported", v))
}

func (tv *TestVars) uniqueString(key string) string {
	return fmt.Sprintf("%s_%s", tv.testName, key)
}

func (tv *TestVars) uuidString(_ string) string {
	return uuid.New()
}

func (tv *TestVars) clone() *TestVars {
	tv2 := newFromName(tv.testName)
	tv.values.Range(func(key, value any) bool {
		tv2.values.Store(key, value)
		return true
	})
	tv.ns.Range(func(key, value any) bool {
		tv2.ns.Store(key, value)
		return true
	})

	return tv2
}
func (tv *TestVars) cloneSetVal(key string, val any) *TestVars {
	tv2 := tv.clone()
	tv2.values.Store(key, val)
	return tv2
}

func (tv *TestVars) cloneSetN(key string, n int) *TestVars {
	tv2 := tv.clone()
	tv2.ns.Store(key, n)
	return tv2
}

// ----------- Methods for every entity ------------
// Add more as you need them following the pattern below.
// Replace "Entity" with the name of the entity, i.e., UpdateID, ActivityType, etc.
// Add only the necessary methods (in most cases only first getter).
/*
func (tv *TestVars) Entity() string {
	return getOrCreate(tv, "entity", tv.uniqueString, tv.stringNSetter)
}
func (tv *TestVars) WithEntity(entity string) *TestVars {
	return tv.cloneSetVal("entity", entity)
}
func (tv *TestVars) WithEntityN(n int) *TestVars {
	return tv.cloneSetN("entity", n)
}
*/

func (tv *TestVars) NamespaceID() namespace.ID {
	return getOrCreate(tv, "namespace_id",
		func(key string) namespace.ID {
			return namespace.ID(tv.uuidString(key))
		},
		func(val namespace.ID, n int) namespace.ID {
			return namespace.ID(tv.uuidNSetter(val.String(), n))
		},
	)
}

func (tv *TestVars) WithNamespaceID(namespaceID namespace.ID) *TestVars {
	return tv.cloneSetVal("namespace_id", namespaceID)
}

func (tv *TestVars) NamespaceName() namespace.Name {
	return getOrCreate(tv, "namespace_name",
		func(key string) namespace.Name {
			return namespace.Name(tv.uniqueString(key))
		},
		func(val namespace.Name, n int) namespace.Name {
			return namespace.Name(tv.stringNSetter(val.String(), n))
		},
	)
}

func (tv *TestVars) WithNamespaceName(namespaceName namespace.Name) *TestVars {
	return tv.cloneSetVal("namespace_name", namespaceName)
}

func (tv *TestVars) Namespace() *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   tv.NamespaceID().String(),
			Name: tv.NamespaceName().String(),
		},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(int32(tv.Any().Int())),
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{
					tv.Any().String(): nil,
				},
			},
		},
		tv.Global().ClusterName(),
	)
}

func (tv *TestVars) WorkflowID() string {
	return getOrCreate(tv, "workflow_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithWorkflowIDN(n int) *TestVars {
	return tv.cloneSetN("workflow_id", n)
}

func (tv *TestVars) RunID() string {
	return getOrCreate(tv, "run_id", tv.uuidString, tv.uuidNSetter)
}

func (tv *TestVars) WithRunID(runID string) *TestVars {
	return tv.cloneSetVal("run_id", runID)
}

func (tv *TestVars) WorkflowExecution() *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      tv.RunID(),
	}
}

func (tv *TestVars) RequestID() string {
	return getOrCreate(tv, "request_id", tv.uuidString, tv.uuidNSetter)
}

func (tv *TestVars) BuildID() string {
	return getOrCreate(tv, "build_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithBuildID(buildId string) *TestVars {
	return tv.cloneSetVal("build_id", buildId)
}

func (tv *TestVars) WithBuildIDN(n int) *TestVars {
	return tv.cloneSetN("build_id", n)
}

func (tv *TestVars) DeploymentSeries() string {
	return getOrCreate(tv, "deployment_series", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithDeploymentSeries(series string) *TestVars {
	return tv.cloneSetVal("deployment_series", series)
}

func (tv *TestVars) Deployment() *deploymentpb.Deployment {
	return &deploymentpb.Deployment{
		SeriesName: tv.DeploymentSeries(),
		BuildId:    tv.BuildID(),
	}
}

func (tv *TestVars) TaskQueue() *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name: getOrCreate(tv, "task_queue", tv.uniqueString, tv.stringNSetter),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
}

func (tv *TestVars) WithTaskQueue(taskQueue string) *TestVars {
	return tv.cloneSetVal("task_queue", taskQueue)
}

func (tv *TestVars) WithTaskQueueN(n int) *TestVars {
	return tv.cloneSetN("task_queue", n)
}

func (tv *TestVars) StickyTaskQueue() *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name:       getOrCreate(tv, "sticky_task_queue", tv.uniqueString, tv.stringNSetter),
		Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
		NormalName: tv.TaskQueue().Name,
	}
}

func (tv *TestVars) StickyExecutionAttributes(timeout time.Duration) *taskqueuepb.StickyExecutionAttributes {
	return &taskqueuepb.StickyExecutionAttributes{
		WorkerTaskQueue:        tv.StickyTaskQueue(),
		ScheduleToStartTimeout: durationpb.New(timeout),
	}
}

func (tv *TestVars) WorkflowType() *commonpb.WorkflowType {
	return &commonpb.WorkflowType{
		Name: getOrCreate(tv, "workflow_type", tv.uniqueString, tv.stringNSetter),
	}
}

func (tv *TestVars) ActivityID() string {
	return getOrCreate(tv, "activity_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithActivityIDN(n int) *TestVars {
	return tv.cloneSetN("activity_id", n)
}

func (tv *TestVars) ActivityType() *commonpb.ActivityType {
	return &commonpb.ActivityType{
		Name: getOrCreate(tv, "activity_type", tv.uniqueString, tv.stringNSetter),
	}
}

func (tv *TestVars) MessageID() string {
	return getOrCreate(tv, "message_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithMessageIDN(n int) *TestVars {
	return tv.cloneSetN("message_id", n)
}

func (tv *TestVars) UpdateID() string {
	return getOrCreate(tv, "update_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithUpdateIDN(n int) *TestVars {
	return tv.cloneSetN("update_id", n)
}

func (tv *TestVars) UpdateRef() *updatepb.UpdateRef {
	return &updatepb.UpdateRef{
		UpdateId:          tv.UpdateID(),
		WorkflowExecution: tv.WorkflowExecution(),
	}
}

func (tv *TestVars) HandlerName() string {
	return getOrCreate(tv, "handler_name", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) ClientIdentity() string {
	return getOrCreate(tv, "client_identity", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WorkerIdentity() string {
	return getOrCreate(tv, "worker_identity", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) TimerID() string {
	return getOrCreate(tv, "timer_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithTimerIDN(n int) *TestVars {
	return tv.cloneSetN("timer_id", n)
}

func (tv *TestVars) QueryType() string {
	return getOrCreate(tv, "query_type", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) SignalName() string {
	return getOrCreate(tv, "signal_name", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) IndexName() string {
	return getOrCreate(tv, "index_name", tv.uniqueString, tv.stringNSetter)
}

// ----------- Generic methods ------------
func (tv *TestVars) Any() Any {
	return tv.an
}

func (tv *TestVars) Global() Global {
	return newGlobal()
}

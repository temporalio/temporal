package testvars

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/worker"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	TestVars struct {
		testName string
		testHash uint32
		an       Any
		values   sync.Map
		numbers  sync.Map
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

	n, ok := tv.numbers.Load(key)
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
	return uuid.NewString()
}

func (tv *TestVars) emptyString(_ string) string {
	return ""
}

func (tv *TestVars) clone() *TestVars {
	tv2 := newFromName(tv.testName)
	tv.values.Range(func(key, value any) bool {
		tv2.values.Store(key, value)
		return true
	})
	tv.numbers.Range(func(key, value any) bool {
		tv2.numbers.Store(key, value)
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
	tv2.numbers.Store(key, n)
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
func (tv *TestVars) WithEntityNumber(n int) *TestVars {
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

func (tv *TestVars) WithWorkflowIDNumber(n int) *TestVars {
	return tv.cloneSetN("workflow_id", n)
}

// RunID is different from other getters. By default, it returns an empty string.
// This is to simplify the usage of WorkflowExecution() which most of the time
// doesn't need RunID. Otherwise, RunID can be set explicitly using WithRunID.
func (tv *TestVars) RunID() string {
	return getOrCreate(tv, "run_id", tv.emptyString, tv.uuidNSetter)
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

func (tv *TestVars) WithRequestID(requestID string) *TestVars {
	return tv.cloneSetVal("request_id", requestID)
}

func (tv *TestVars) BuildID() string {
	return getOrCreate(tv, "build_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithBuildIDNumber(n int) *TestVars {
	return tv.cloneSetN("build_id", n)
}

// [cleanup-wv-pre-release]
func (tv *TestVars) DeploymentSeries() string {
	return getOrCreate(tv, "deployment_series", tv.uniqueString, tv.stringNSetter)
}

// [cleanup-wv-pre-release]
func (tv *TestVars) WithDeploymentSeriesNumber(n int) *TestVars {
	return tv.cloneSetN("deployment_series", n)
}

// [cleanup-wv-pre-release]
func (tv *TestVars) Deployment() *deploymentpb.Deployment {
	return &deploymentpb.Deployment{
		SeriesName: tv.DeploymentSeries(),
		BuildId:    tv.BuildID(),
	}
}

func (tv *TestVars) DeploymentVersion() *deploymentspb.WorkerDeploymentVersion {
	return &deploymentspb.WorkerDeploymentVersion{
		BuildId:        tv.BuildID(),
		DeploymentName: tv.DeploymentSeries(),
	}
}

func (tv *TestVars) ExternalDeploymentVersion() *deploymentpb.WorkerDeploymentVersion {
	return &deploymentpb.WorkerDeploymentVersion{
		BuildId:        tv.BuildID(),
		DeploymentName: tv.DeploymentSeries(),
	}
}

// SDKDeploymentVersion returns SDK worker deployment version
func (tv *TestVars) SDKDeploymentVersion() worker.WorkerDeploymentVersion {
	return worker.WorkerDeploymentVersion{
		BuildID:        tv.BuildID(),
		DeploymentName: tv.DeploymentSeries(),
	}
}

// DeploymentVersionString returns v31 string
func (tv *TestVars) DeploymentVersionString() string {
	return worker_versioning.WorkerDeploymentVersionToStringV31(tv.DeploymentVersion())
}

func (tv *TestVars) DeploymentVersionStringV32() string {
	return worker_versioning.ExternalWorkerDeploymentVersionToString(tv.ExternalDeploymentVersion())
}

func (tv *TestVars) DeploymentVersionTransition() *workflowpb.DeploymentVersionTransition {
	ret := &workflowpb.DeploymentVersionTransition{
		DeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromVersion(tv.DeploymentVersion()),
	}
	// DescribeWorkflowExecution populates both fields on read, so we expect to see both fields
	//nolint:staticcheck // SA1019: worker versioning v0.31
	ret.Version = worker_versioning.ExternalWorkerDeploymentVersionToStringV31(ret.GetDeploymentVersion())
	return ret
}

func (tv *TestVars) VersioningOverridePinned(useV32 bool) *workflowpb.VersioningOverride {
	if useV32 {
		return &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  tv.ExternalDeploymentVersion(),
				},
			},
		}
	}
	return &workflowpb.VersioningOverride{
		Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
		PinnedVersion: tv.DeploymentVersionString(),
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

func (tv *TestVars) WithDeploymentSeries(deploymentSeries string) *TestVars {
	return tv.cloneSetVal("deployment_series", deploymentSeries)
}

func (tv *TestVars) WithBuildID(buildID string) *TestVars {
	return tv.cloneSetVal("build_id", buildID)
}

func (tv *TestVars) WithWorkflowID(workflowID string) *TestVars {
	return tv.cloneSetVal("workflow_id", workflowID)
}

func (tv *TestVars) WithTaskQueueNumber(n int) *TestVars {
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

func (tv *TestVars) WithActivityIDNumber(n int) *TestVars {
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

func (tv *TestVars) WithMessageIDNumber(n int) *TestVars {
	return tv.cloneSetN("message_id", n)
}

func (tv *TestVars) UpdateID() string {
	return getOrCreate(tv, "update_id", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) WithUpdateIDNumber(n int) *TestVars {
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

func (tv *TestVars) WithTimerIDNumber(n int) *TestVars {
	return tv.cloneSetN("timer_id", n)
}

func (tv *TestVars) QueryType() string {
	return getOrCreate(tv, "query_type", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) Query() *querypb.WorkflowQuery {
	return &querypb.WorkflowQuery{
		QueryType: tv.QueryType(),
		QueryArgs: tv.Any().Payloads(),
	}
}

func (tv *TestVars) SignalName() string {
	return getOrCreate(tv, "signal_name", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) IndexName() string {
	return getOrCreate(tv, "index_name", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) Service() string {
	return getOrCreate(tv, "service", tv.uniqueString, tv.stringNSetter)
}

func (tv *TestVars) Operation() string {
	return getOrCreate(tv, "operation", tv.uniqueString, tv.stringNSetter)
}

// ----------- Generic methods ------------
func (tv *TestVars) Any() Any {
	return tv.an
}

func (tv *TestVars) Global() Global {
	return newGlobal()
}

func (tv *TestVars) WorkerDeploymentOptions(versioned bool) *deploymentpb.WorkerDeploymentOptions {
	m := enumspb.WORKER_VERSIONING_MODE_UNVERSIONED
	if versioned {
		m = enumspb.WORKER_VERSIONING_MODE_VERSIONED
	}
	return &deploymentpb.WorkerDeploymentOptions{
		BuildId:              tv.BuildID(),
		DeploymentName:       tv.DeploymentSeries(),
		WorkerVersioningMode: m,
	}
}

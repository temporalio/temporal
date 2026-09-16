package all_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/all"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRegisterAll(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	// Decoding starts from the root node's component type ID, so every archetype must resolve.
	for _, archetype := range []chasm.Archetype{
		chasm.WorkflowArchetype,
		chasm.SchedulerArchetype,
		activity.Archetype,
	} {
		id, ok := registry.ComponentIDByFqn(archetype)
		require.True(t, ok, "archetype %s not registered", archetype)

		_, ok = registry.ComponentByID(id)
		require.True(t, ok, "archetype %s has no registrable component", archetype)
	}
}

// TestDetachedRead_StandaloneActivity covers what an offline reader has: persisted nodes,
// execution metadata, and a registry with nil handlers.
func TestDetachedRead_StandaloneActivity(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	mutableState := persistStandaloneActivity(t, registry)

	// Archetype is readable before decoding, so a caller can dispatch on it.
	archetype, err := chasm.RootArchetype(mutableState, registry)
	require.NoError(t, err)
	require.Equal(t, activity.Archetype, archetype)

	act, ctx, err := chasm.DetachedRootComponent[*activity.Activity](context.Background(), mutableState, registry)
	require.NoError(t, err)

	// State decoded from the root node and its data children.
	require.Equal(t, "MyActivity", act.GetActivityType().GetName())
	require.Equal(t, "my-tq", act.GetTaskQueue().GetName())
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED, act.GetStatus())
	require.Equal(t, int32(3), act.LastAttempt.Get(ctx).GetCount())
	require.NotNil(t, act.Outcome.Get(ctx).GetSuccessful())
	require.True(t, act.LifecycleState(ctx).IsClosed())

	// Execution facts come from the record the caller supplied, not from the tree.
	require.Equal(t, "ns-id", ctx.ExecutionKey().NamespaceID)
	require.Equal(t, "my-activity-id", ctx.ExecutionKey().BusinessID)
	require.Equal(t, "run-id-1", ctx.ExecutionKey().RunID)
	require.Equal(t, time.Unix(900, 0).UTC(), ctx.ExecutionInfo().CloseTime)
	require.Equal(t, int64(7), ctx.ExecutionInfo().StateTransitionCount)
}

// TestDetachedRead_WritePathPanics documents the read only contract: a write path panics
// rather than returning a result computed against a fabricated backend.
func TestDetachedRead_WritePathPanics(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	root, err := chasm.NewDetachedTree(persistStandaloneActivity(t, registry), registry)
	require.NoError(t, err)

	require.Panics(t, func() {
		_, _ = root.CloseTransaction()
	})
}

// TestDetachedRootComponent_Interface covers instantiating C as an interface rather than a
// concrete type, which is how a caller reaches DescribeComponent without naming the root's
// type. The constraint stays Component, not RootComponent, so an interface like
// DescribableComponent can be used here.
func TestDetachedRootComponent_Interface(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	component, ctx, err := chasm.DetachedRootComponent[chasm.Component](
		context.Background(),
		persistStandaloneActivity(t, registry),
		registry,
	)
	require.NoError(t, err)
	require.True(t, component.LifecycleState(ctx).IsClosed())
}

// TestDetachedRootComponent_WrongType reports the mismatch rather than handing back a zero
// value.
func TestDetachedRootComponent_WrongType(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	_, _, err = chasm.DetachedRootComponent[*chasm.Visibility](
		context.Background(),
		persistStandaloneActivity(t, registry),
		registry,
	)
	require.ErrorContains(t, err, "root component is *activity.Activity")
}

// persistStandaloneActivity returns the mutable state record a closed standalone activity
// lands in persistence as.
func persistStandaloneActivity(t *testing.T, registry *chasm.Registry) *persistencespb.WorkflowMutableState {
	t.Helper()

	logger := log.NewTestLogger()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(1000, 0).UTC())

	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("ns-id", "my-activity-id", "run-id-1")
		},
		HandleGetExecutionInfo:    func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} },
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
	}

	root := chasm.NewEmptyTree(registry, timeSource, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	mutableCtx := chasm.NewMutableContext(context.Background(), root)

	act := &activity.Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			ActivityType:           &commonpb.ActivityType{Name: "MyActivity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "my-tq"},
			ScheduleTime:           timestamppb.New(time.Unix(100, 0).UTC()),
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
		},
		Visibility:  chasm.NewComponentField(mutableCtx, chasm.NewVisibility(mutableCtx)),
		LastAttempt: chasm.NewDataField(mutableCtx, &activitypb.ActivityAttemptState{Count: 3}),
		RequestData: chasm.NewDataField(mutableCtx, &activitypb.ActivityRequestData{}),
		Outcome: chasm.NewDataField(mutableCtx, &activitypb.ActivityOutcome{
			Variant: &activitypb.ActivityOutcome_Successful_{
				Successful: &activitypb.ActivityOutcome_Successful{},
			},
		}),
	}
	require.NoError(t, root.SetRootComponent(act))
	_, err := root.CloseTransaction()
	require.NoError(t, err)

	// Shaped the way a caller that persisted the nodes would store them: the tree, plus the
	// execution facts that live on the record rather than in the tree.
	return &persistencespb.WorkflowMutableState{
		ChasmNodes: root.Snapshot(nil).Nodes,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:          "ns-id",
			WorkflowId:           "my-activity-id",
			CloseTime:            timestamppb.New(time.Unix(900, 0).UTC()),
			StateTransitionCount: 7,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run-id-1"},
	}
}

// TestDetachedRead_NodesOnly reads a record carrying only nodes. Decoding and component state
// are unaffected; only the execution facts, which are not in the tree, come back zero.
func TestDetachedRead_NodesOnly(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	nodesOnly := &persistencespb.WorkflowMutableState{
		ChasmNodes: persistStandaloneActivity(t, registry).GetChasmNodes(),
	}

	act, ctx, err := chasm.DetachedRootComponent[*activity.Activity](
		context.Background(), nodesOnly, registry)
	require.NoError(t, err)

	// The component's own state is all there.
	require.Equal(t, "MyActivity", act.GetActivityType().GetName())
	require.Equal(t, "my-tq", act.GetTaskQueue().GetName())
	require.Equal(t, int32(3), act.LastAttempt.Get(ctx).GetCount())
	require.True(t, act.LifecycleState(ctx).IsClosed())

	// What the record did not carry reads as zero, not as a panic or an error.
	require.Equal(t, chasm.ExecutionKey{}, ctx.ExecutionKey())
	require.Zero(t, ctx.ExecutionInfo().CloseTime)
	require.Zero(t, ctx.ExecutionInfo().StateTransitionCount)
}

// TestNewDetachedTree_NilMutableState is the one input the constructor rejects.
func TestNewDetachedTree_NilMutableState(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	_, err = chasm.NewDetachedTree(nil, registry)
	require.ErrorContains(t, err, "nil")
}

// TestRegisterAll_TasksDecodable walks the tasks a real tree carries and checks each resolves
// to a proto type and decodes. Nil libraries keep their Tasks(), so offline readers can render
// logical tasks, and a library whose nil constructor dropped them would fail here.
func TestRegisterAll_TasksDecodable(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	found := 0
	for path, node := range persistStandaloneActivity(t, registry).GetChasmNodes() {
		attributes := node.GetMetadata().GetComponentAttributes()
		if attributes == nil {
			continue
		}

		tasks := append([]*persistencespb.ChasmComponentAttributes_Task{}, attributes.GetSideEffectTasks()...)
		tasks = append(tasks, attributes.GetPureTasks()...)

		for _, task := range tasks {
			found++

			fqn, ok := registry.TaskFqnByID(task.GetTypeId())
			require.True(t, ok, "task type %d at %q does not resolve", task.GetTypeId(), path)
			require.NotEmpty(t, fqn)

			registrable, ok := registry.TaskByID(task.GetTypeId())
			require.True(t, ok, "task %s does not resolve to a registrable task", fqn)
			require.NotNil(t, registrable.GoType(), "task %s has no Go type to decode into", fqn)

			message, isProto := reflect.New(registrable.GoType().Elem()).Interface().(proto.Message)
			require.True(t, isProto, "task %s Go type is not a proto message", fqn)

			if blob := task.GetData(); blob != nil && len(blob.GetData()) > 0 {
				require.NoError(t, serialization.Decode(blob, message.ProtoReflect().New().Interface()),
					"task %s did not decode", fqn)
			}
		}
	}
	require.NotZero(t, found, "tree carried no tasks, so this asserted nothing")
}

// TestDetachedRead_UnknownTaskType keeps a detached read working when the tree carries a task
// type the reader does not know, as happens when a newer server wrote the record. Tasks are
// inert here: nothing validates or executes them, so an unrecognized one is ignored.
func TestDetachedRead_UnknownTaskType(t *testing.T) {
	registry, err := all.NewRegistry(log.NewTestLogger())
	require.NoError(t, err)

	mutableState := persistStandaloneActivity(t, registry)

	const unknownTaskType = 4294967290
	_, known := registry.TaskByID(unknownTaskType)
	require.False(t, known, "type must be unknown for this test to mean anything")

	attributes := mutableState.GetChasmNodes()[""].GetMetadata().GetComponentAttributes()
	attributes.SideEffectTasks = append(attributes.SideEffectTasks, &persistencespb.ChasmComponentAttributes_Task{
		TypeId: unknownTaskType,
		Data: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte{0x08, 0x2a},
		},
	})

	act, ctx, err := chasm.DetachedRootComponent[*activity.Activity](
		context.Background(), mutableState, registry)
	require.NoError(t, err)
	require.Equal(t, "MyActivity", act.GetActivityType().GetName())
	require.True(t, act.LifecycleState(ctx).IsClosed())
}

// TestAllNilLibrariesRegistered guards RegisterAll against drift: every package under
// chasm/lib exporting NewNilLibrary must appear in the libs slice, and vice versa.
//
// Missing one is not silent (decoding fails with "unknown component type ID"), but it fails in
// tdbg or another offline reader. This moves that discovery to CI.
func TestAllNilLibrariesRegistered(t *testing.T) {
	onDisk := packagesExportingNewNilLibrary(t, "..")
	registered := packagesRegisteredInAllGo(t, "all.go")

	require.Equal(t, onDisk, registered,
		"chasm/lib/all/all.go is out of sync with the libraries on disk.\n"+
			"Every package under chasm/lib exporting NewNilLibrary() must have a line in the libs slice.")
}

// packagesExportingNewNilLibrary returns packages directly under libDir exporting a top-level
// NewNilLibrary.
func packagesExportingNewNilLibrary(t *testing.T, libDir string) []string {
	t.Helper()

	entries, err := os.ReadDir(libDir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	var found []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pkgDir := filepath.Join(libDir, entry.Name())

		files, err := os.ReadDir(pkgDir)
		require.NoError(t, err)

		for _, file := range files {
			name := file.Name()
			if file.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}

			parsed, err := parser.ParseFile(fset, filepath.Join(pkgDir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				continue
			}
			if declaresFunc(parsed, "NewNilLibrary") {
				found = append(found, entry.Name())
				break
			}
		}
	}

	slices.Sort(found)
	return found
}

// packagesRegisteredInAllGo returns package names appearing as <pkg>.NewNilLibrary() in file.
func packagesRegisteredInAllGo(t *testing.T, path string) []string {
	t.Helper()

	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	var registered []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "NewNilLibrary" {
			return true
		}
		pkg, isIdent := selector.X.(*ast.Ident)
		if !isIdent {
			return true
		}
		registered = append(registered, pkg.Name)
		return true
	})

	slices.Sort(registered)
	return registered
}

func declaresFunc(file *ast.File, name string) bool {
	for _, decl := range file.Decls {
		fn, isFn := decl.(*ast.FuncDecl)
		if isFn && fn.Recv == nil && fn.Name.Name == name {
			return true
		}
	}
	return false
}

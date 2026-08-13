package protocol_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire1"
	v1action "go.temporal.io/server/tests/umpire1/action"
	v1fact "go.temporal.io/server/tests/umpire1/fact"
	v1model "go.temporal.io/server/tests/umpire1/model"
	v1planner "go.temporal.io/server/tests/umpire1/planner"
	umpirev2 "go.temporal.io/server/tests/umpire2"
	v2action "go.temporal.io/server/tests/umpire2/action"
	v2fact "go.temporal.io/server/tests/umpire2/fact"
	v2model "go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/protocol"
)

func TestDefaultMonitorRulesContainActiveV1Rules(t *testing.T) {
	v1, err := umpire1.NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)
	v2, err := umpirev2.NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)

	v2Rules := make(map[string]string)
	for _, stats := range v2.RuleStats() {
		v2Rules[stats.Name] = stats.Kind
	}
	for _, stats := range v1.RuleStats() {
		require.Equalf(t, stats.Kind, v2Rules[stats.Name], "missing or mismatched v1 rule %s", stats.Name)
	}
}

func TestDefaultNexusPlansMatchV1ForEveryEdgeAndHosting(t *testing.T) {
	protocol, err := protocol.Default()
	require.NoError(t, err)
	lifecycle, ok := v1planner.DefaultModels().Lifecycle(string(v1model.NexusOperationType))
	require.True(t, ok)

	for _, edge := range lifecycle.Edges() {
		for _, hosting := range []umpire.Hosting{umpire.Standalone, umpire.Embedded} {
			t.Run(edge.From+"/"+edge.Event+"/"+hosting.String(), func(t *testing.T) {
				want, wantErr := v1action.PlanEdge(edge.From, edge.Event, hosting)
				got, gotErr := protocol.PlanEdge(v2model.NexusOperationType, edge.From, edge.Event, hosting)

				require.Equal(t, wantErr != nil, gotErr != nil)
				if wantErr == nil {
					require.Equal(t, describeActions(want), describeActions(got))
				}
			})
		}
	}
}

func TestDefaultWorkflowPlansMatchV1ForEveryEdge(t *testing.T) {
	protocol, err := protocol.Default()
	require.NoError(t, err)
	lifecycle, ok := v1planner.DefaultModels().Lifecycle(string(v1model.WorkflowType))
	require.True(t, ok)

	for _, edge := range lifecycle.Edges() {
		t.Run(edge.From+"/"+edge.Event, func(t *testing.T) {
			want, wantErr := v1action.WorkflowPlanEdge(edge.From, edge.Event)
			got, gotErr := protocol.PlanEdge(v2model.WorkflowType, edge.From, edge.Event, umpire.Standalone)

			require.Equal(t, wantErr != nil, gotErr != nil)
			if wantErr == nil {
				require.Equal(t, describeActions(want), describeActions(got))
			}
		})
	}
}

func TestDefaultAsyncCompletionPayloadsMatchV1(t *testing.T) {
	protocol, err := protocol.Default()
	require.NoError(t, err)

	for _, event := range []string{v1model.NexusFail, v1model.NexusCancel} {
		t.Run(event, func(t *testing.T) {
			v1Plan, err := v1action.PlanEdge(v1model.NexusStarted, event, umpire.Standalone)
			require.NoError(t, err)
			v2Plan, err := protocol.PlanEdge(
				v2model.NexusOperationType,
				v2model.NexusStarted,
				event,
				umpire.Standalone,
			)
			require.NoError(t, err)

			want := realizeCompletionPayload(t, v1Plan[len(v1Plan)-1], func() (nexustest.Handler, umpire.RealizeContext) {
				policy := v1action.NewResponsePolicy()
				return policy.Handler(), &v1action.Ctx{Handler: policy}
			})
			got := realizeCompletionPayload(t, v2Plan[len(v2Plan)-1], func() (nexustest.Handler, umpire.RealizeContext) {
				policy := v2action.NewResponsePolicy()
				return policy.Handler(), &v2action.Ctx{Handler: policy}
			})
			require.Equal(t, want, got)
		})
	}
}

func TestDefaultMonitorRegistrationMatchesV1ForTargetedAndBroadcastFacts(t *testing.T) {
	protocol, err := protocol.Default()
	require.NoError(t, err)
	v1 := umpire.NewModelState()
	v1model.RegisterDefaultEntities(v1)
	v2 := umpire.NewModelState()
	protocol.Register(v2)

	v1Path := &umpire.EntityPath{
		EntityID: umpire.NewEntityID(v1model.WorkflowTaskType, "queue:workflow:run"),
		Ancestors: []umpire.EntityID{
			umpire.NewEntityID(v1model.NamespaceType, "namespace"),
			umpire.NewEntityID(v1model.WorkflowType, "workflow"),
		},
	}
	v2Path := &umpire.EntityPath{
		EntityID: umpire.NewEntityID(v2model.WorkflowTaskType, "queue:workflow:run"),
		Ancestors: []umpire.EntityID{
			umpire.NewEntityID(v2model.NamespaceType, "namespace"),
			umpire.NewEntityID(v2model.WorkflowType, "workflow"),
		},
	}
	v1Scheduled := &v1fact.SpeculativeWorkflowTaskScheduled{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
		TaskQueue:   "queue",
		EntityPath:  v1Path,
	}
	v1Terminated := &v1fact.WorkflowTerminated{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
	}
	v2Scheduled := &v2fact.SpeculativeWorkflowTaskScheduled{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
		TaskQueue:   "queue",
		EntityPath:  v2Path,
	}
	v2Terminated := &v2fact.WorkflowTerminated{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
	}
	require.NoError(t, v1.RouteFacts(context.Background(), []umpire.Fact{v1Scheduled, v1Terminated}))
	require.NoError(t, v2.RouteFacts(context.Background(), []umpire.Fact{v2Scheduled, v2Terminated}))

	want := v1WorkflowTaskDescriptionFor(t, v1)
	got := v2WorkflowTaskDescriptionFor(t, v2)
	require.Equal(t, []string{"Workflow", "WorkflowTask"}, registeredEntityTypes(v1))
	require.Equal(t, registeredEntityTypes(v1), registeredEntityTypes(v2))
	require.Equal(t, workflowTaskDescription{
		TaskQueue:     "queue",
		WorkflowID:    "workflow",
		RunID:         "run",
		NamespaceID:   "namespace",
		IsSpeculative: true,
		State:         v1model.TaskTerminated,
	}, want)
	require.Equal(t, want, got)
}

func registeredEntityTypes(modelState *umpire.ModelState) []string {
	entries := modelState.QueryAll(0, nil)
	types := make([]string, len(entries))
	for i, entry := range entries {
		types[i] = string(entry.Entity.Type())
	}
	slices.Sort(types)
	return types
}

func realizeCompletionPayload(
	t *testing.T,
	executable umpire.Action,
	newHandler func() (nexustest.Handler, umpire.RealizeContext),
) any {
	t.Helper()
	type result struct {
		body []byte
		err  error
	}
	requests := make(chan result, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		requests <- result{body: body, err: err}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	handler, realizeContext := newHandler()
	_, err := handler.OnStartOperation(
		context.Background(),
		"service",
		"operation",
		nil,
		nexus.StartOperationOptions{
			CallbackURL: server.URL,
			CallbackHeader: nexus.Header{
				commonnexus.CallbackTokenHeader: "callback-token",
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, executable.Realize.Fire(
		context.Background(),
		realizeContext,
		executable,
	))
	captured := <-requests
	require.NoError(t, captured.err)

	var payload any
	require.NoError(t, json.Unmarshal(captured.body, &payload))
	return payload
}

type actionDescription struct {
	Name         string
	Kind         umpire.Kind
	Hosting      umpire.Hosting
	Requires     []umpire.Pre
	Effects      []umpire.Effect
	Entry        []string
	Footprint    []string
	Reject       *umpire.Reject
	RealizerType string
}

func describeActions(actions []umpire.Action) []actionDescription {
	descriptions := make([]actionDescription, len(actions))
	for i, executable := range actions {
		descriptions[i] = actionDescription{
			Name:         executable.Name,
			Kind:         executable.Kind,
			Hosting:      executable.Hosting,
			Requires:     executable.Requires,
			Effects:      executable.Effects,
			Entry:        executable.Entry,
			Footprint:    executable.Footprint,
			Reject:       executable.Reject,
			RealizerType: realizerType(executable.Realize),
		}
	}
	return descriptions
}

func realizerType(realizer umpire.Realizer) string {
	if realizer == nil {
		return ""
	}
	realizerType := reflect.TypeOf(realizer)
	for realizerType.Kind() == reflect.Pointer {
		realizerType = realizerType.Elem()
	}
	return realizerType.Kind().String() + ":" + realizerType.Name()
}

type workflowTaskDescription struct {
	TaskQueue     string
	WorkflowID    string
	RunID         string
	NamespaceID   string
	IsSpeculative bool
	State         string
}

func v1WorkflowTaskDescriptionFor(t *testing.T, modelState *umpire.ModelState) workflowTaskDescription {
	t.Helper()
	entries := modelState.QueryEntities(v1model.WorkflowTaskType, 0, nil)
	require.Len(t, entries, 1)
	workflowTask, ok := entries[0].Entity.(*v1model.WorkflowTask)
	require.True(t, ok)
	return workflowTaskDescription{
		TaskQueue:     workflowTask.TaskQueue,
		WorkflowID:    workflowTask.WorkflowID,
		RunID:         workflowTask.RunID,
		NamespaceID:   workflowTask.NamespaceID,
		IsSpeculative: workflowTask.IsSpeculative,
		State:         workflowTask.Lifecycle().Current(),
	}
}

func v2WorkflowTaskDescriptionFor(t *testing.T, modelState *umpire.ModelState) workflowTaskDescription {
	t.Helper()
	entries := modelState.QueryEntities(v2model.WorkflowTaskType, 0, nil)
	require.Len(t, entries, 1)
	workflowTask, ok := entries[0].Entity.(*v2model.WorkflowTask)
	require.True(t, ok)
	return workflowTaskDescription{
		TaskQueue:     workflowTask.TaskQueue,
		WorkflowID:    workflowTask.WorkflowID,
		RunID:         workflowTask.RunID,
		NamespaceID:   workflowTask.NamespaceID,
		IsSpeculative: workflowTask.IsSpeculative,
		State:         workflowTask.Lifecycle().Current(),
	}
}

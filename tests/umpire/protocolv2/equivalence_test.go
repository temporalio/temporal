package protocolv2_test

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
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/action"
	"go.temporal.io/server/tests/umpire/fact"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
	"go.temporal.io/server/tests/umpire/protocolv2"
)

func TestDefaultNexusPlansMatchV1ForEveryEdgeAndHosting(t *testing.T) {
	protocol, err := protocolv2.Default()
	require.NoError(t, err)
	lifecycle, ok := planner.DefaultModels().Lifecycle(string(model.NexusOperationType))
	require.True(t, ok)

	for _, edge := range lifecycle.Edges() {
		for _, hosting := range []umpire.Hosting{umpire.Standalone, umpire.Embedded} {
			t.Run(edge.From+"/"+edge.Event+"/"+hosting.String(), func(t *testing.T) {
				want, wantErr := action.PlanEdge(edge.From, edge.Event, hosting)
				got, gotErr := protocol.PlanEdge(model.NexusOperationType, edge.From, edge.Event, hosting)

				require.Equal(t, wantErr != nil, gotErr != nil)
				if wantErr == nil {
					require.Equal(t, describeActions(want), describeActions(got))
				}
			})
		}
	}
}

func TestDefaultWorkflowPlansMatchV1ForEveryEdge(t *testing.T) {
	protocol, err := protocolv2.Default()
	require.NoError(t, err)
	lifecycle, ok := planner.DefaultModels().Lifecycle(string(model.WorkflowType))
	require.True(t, ok)

	for _, edge := range lifecycle.Edges() {
		t.Run(edge.From+"/"+edge.Event, func(t *testing.T) {
			want, wantErr := action.WorkflowPlanEdge(edge.From, edge.Event)
			got, gotErr := protocol.PlanEdge(model.WorkflowType, edge.From, edge.Event, umpire.Standalone)

			require.Equal(t, wantErr != nil, gotErr != nil)
			if wantErr == nil {
				require.Equal(t, describeActions(want), describeActions(got))
			}
		})
	}
}

func TestDefaultAsyncCompletionPayloadsMatchV1(t *testing.T) {
	protocol, err := protocolv2.Default()
	require.NoError(t, err)

	for _, event := range []string{model.NexusFail, model.NexusCancel} {
		t.Run(event, func(t *testing.T) {
			v1Plan, err := action.PlanEdge(model.NexusStarted, event, umpire.Standalone)
			require.NoError(t, err)
			v2Plan, err := protocol.PlanEdge(
				model.NexusOperationType,
				model.NexusStarted,
				event,
				umpire.Standalone,
			)
			require.NoError(t, err)

			want := realizeCompletionPayload(t, v1Plan[len(v1Plan)-1])
			got := realizeCompletionPayload(t, v2Plan[len(v2Plan)-1])
			require.Equal(t, want, got)
		})
	}
}

func TestDefaultMonitorRegistrationMatchesV1ForTargetedAndBroadcastFacts(t *testing.T) {
	protocol, err := protocolv2.Default()
	require.NoError(t, err)
	v1 := umpire.NewModelState()
	model.RegisterDefaultEntities(v1)
	v2 := umpire.NewModelState()
	protocol.Register(v2)

	path := &umpire.EntityPath{
		EntityID: umpire.NewEntityID(model.WorkflowTaskType, "queue:workflow:run"),
		Ancestors: []umpire.EntityID{
			umpire.NewEntityID(model.NamespaceType, "namespace"),
			umpire.NewEntityID(model.WorkflowType, "workflow"),
		},
	}
	scheduled := &fact.SpeculativeWorkflowTaskScheduled{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
		TaskQueue:   "queue",
		EntityPath:  path,
	}
	terminated := &fact.WorkflowTerminated{
		WorkflowID:  "workflow",
		RunID:       "run",
		NamespaceID: "namespace",
	}
	for _, modelState := range []*umpire.ModelState{v1, v2} {
		require.NoError(t, modelState.RouteFacts(context.Background(), []umpire.Fact{scheduled}))
		require.NoError(t, modelState.RouteFacts(context.Background(), []umpire.Fact{terminated}))
	}

	want := workflowTaskDescriptionFor(t, v1)
	got := workflowTaskDescriptionFor(t, v2)
	require.Equal(t, []string{"Workflow", "WorkflowTask"}, registeredEntityTypes(v1))
	require.Equal(t, registeredEntityTypes(v1), registeredEntityTypes(v2))
	require.Equal(t, workflowTaskDescription{
		TaskQueue:     "queue",
		WorkflowID:    "workflow",
		RunID:         "run",
		NamespaceID:   "namespace",
		IsSpeculative: true,
		State:         model.TaskTerminated,
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

func realizeCompletionPayload(t *testing.T, executable umpire.Action) any {
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

	policy := action.NewResponsePolicy()
	handler := policy.Handler()
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
		&action.Ctx{Handler: policy},
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
	RealizerType reflect.Type
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
			RealizerType: reflect.TypeOf(executable.Realize),
		}
	}
	return descriptions
}

type workflowTaskDescription struct {
	TaskQueue     string
	WorkflowID    string
	RunID         string
	NamespaceID   string
	IsSpeculative bool
	State         string
}

func workflowTaskDescriptionFor(t *testing.T, modelState *umpire.ModelState) workflowTaskDescription {
	t.Helper()
	entries := modelState.QueryEntities(model.WorkflowTaskType, 0, nil)
	require.Len(t, entries, 1)
	workflowTask, ok := entries[0].Entity.(*model.WorkflowTask)
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

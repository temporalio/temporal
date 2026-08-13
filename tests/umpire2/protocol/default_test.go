package protocol

import (
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/planner"
)

func TestDefaultMatchesConcreteEntityAndLifecycleCatalogs(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	wantTypes := make([]string, 0, len(model.DefaultEntities()))
	for _, entity := range model.DefaultEntities() {
		entityType := entity.New().Type()
		wantTypes = append(wantTypes, string(entityType))
		compiled, ok := protocol.entities[entityType]
		require.True(t, ok, "missing entity %s", entityType)
		require.Equal(t, factTypes(entity.Facts), factTypes(compiled.facts))

		wantLifecycle, lifecycled := entity.New().(umpire.Lifecycled)
		gotLifecycle, planned := protocol.Lifecycle(entityType)
		require.Equal(t, lifecycled, planned, "entity %s lifecycle classification", entityType)
		if lifecycled {
			require.Equal(t, lifecycleCatalog(wantLifecycle.Lifecycle()), lifecycleCatalog(gotLifecycle))
			concreteLifecycle, ok := planner.DefaultModels().Lifecycle(string(entityType))
			require.True(t, ok)
			require.Equal(t, lifecycleCatalog(concreteLifecycle), lifecycleCatalog(gotLifecycle))
		}
	}
	slices.Sort(wantTypes)
	gotTypes := make([]string, 0, len(protocol.entities))
	for entityType := range protocol.entities {
		gotTypes = append(gotTypes, string(entityType))
	}
	slices.Sort(gotTypes)
	require.Equal(t, wantTypes, gotTypes)
}

func TestDefaultMatchesModelStateFactSet(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	want := []string{
		"WorkflowStarted",
		"WorkflowExecutionClosed",
		"WorkflowRunStarted",
		"WorkflowRunClosed",
		"WorkflowTaskAdded",
		"WorkflowTaskPolled",
		"WorkflowTaskStored",
		"WorkflowTaskDiscarded",
		"WorkflowTerminated",
		"SpeculativeWorkflowTaskScheduled",
		"WorkflowNexusStorageSnapshot",
		"NexusOperationScheduled",
		"NexusOperationAttemptFailed",
		"NexusOperationStarted",
		"NexusOperationSucceeded",
		"NexusOperationFailed",
		"NexusOperationCanceled",
		"NexusOperationTimedOut",
		"NexusOperationRejected",
		"NexusOperationCancelRequestFailed",
		"NexusOperationExecutionSnapshot",
		"NexusOperationHistorySnapshot",
		"NexusOperationStartedHistory",
		"NexusOperationTerminal",
		"ActivityExecutionSnapshot",
		"NexusCallbackObservation",
		"NexusStartResponse",
		"WorkflowCallbackAttachment",
	}
	got := make([]string, len(protocol.facts))
	for i, fact := range protocol.facts {
		got[i] = fact.Name()
	}

	require.ElementsMatch(t, want, got)
	require.NotContains(t, got, "chasm.transition")
}

func TestDefaultReturnsFreshCompiledProtocols(t *testing.T) {
	first, err := Default()
	require.NoError(t, err)
	second, err := Default()
	require.NoError(t, err)
	require.NotSame(t, first, second)

	key := ActionKey{
		Entity:  model.WorkflowType,
		From:    model.WorkflowCreated,
		Event:   model.WorkflowStart,
		Hosting: umpire.Standalone,
	}
	delete(first.actions, key)
	_, firstFound := first.Action(key)
	_, secondFound := second.Action(key)

	require.False(t, firstFound)
	require.True(t, secondFound)
}

func TestDefaultEntitySubscriptionsBelongToFactSet(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)
	registered := make(map[reflect.Type]struct{}, len(protocol.facts))
	for _, fact := range protocol.facts {
		registered[reflect.TypeOf(fact)] = struct{}{}
	}

	for entityType, entity := range protocol.entities {
		for _, subscription := range entity.facts {
			_, found := registered[reflect.TypeOf(subscription)]
			require.Truef(t, found, "entity %s subscription %T is not registered", entityType, subscription)
		}
	}
}

func TestDefaultClassifiesEveryActiveEdgeAndHosting(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	tests := []struct {
		entityType umpire.EntityType
		hostings   []umpire.Hosting
	}{
		{entityType: model.WorkflowType, hostings: []umpire.Hosting{umpire.Standalone}},
		{entityType: model.NexusOperationType, hostings: []umpire.Hosting{umpire.Standalone, umpire.Embedded}},
	}
	for _, test := range tests {
		lifecycle, ok := protocol.Lifecycle(test.entityType)
		require.True(t, ok)
		for _, edge := range lifecycle.Edges() {
			for _, hosting := range test.hostings {
				key := ActionKey{Entity: test.entityType, From: edge.From, Event: edge.Event, Hosting: hosting}
				_, actionExists := protocol.actions[key]
				_, gapExists := protocol.gaps[key]
				require.NotEqualf(t, actionExists, gapExists, "key must have exactly one classification: %+v", key)
			}
		}
	}
}

func factTypes(facts []umpire.Fact) []reflect.Type {
	types := make([]reflect.Type, len(facts))
	for i, fact := range facts {
		types[i] = reflect.TypeOf(fact)
	}
	return types
}

type lifecycleDescription struct {
	Initial      string
	States       []string
	Events       []string
	Edges        []umpire.Edge
	StateTraits  map[string]umpire.Traits
	EdgeTraits   map[[2]string]umpire.Traits
	Terminal     map[string]bool
	Dispositions map[string]umpire.Disposition
}

func lifecycleCatalog(lifecycle *umpire.Lifecycle) lifecycleDescription {
	description := lifecycleDescription{
		Initial:      lifecycle.Initial(),
		States:       lifecycle.States(),
		Events:       lifecycle.Events(),
		Edges:        lifecycle.Edges(),
		StateTraits:  make(map[string]umpire.Traits),
		EdgeTraits:   make(map[[2]string]umpire.Traits),
		Terminal:     make(map[string]bool),
		Dispositions: make(map[string]umpire.Disposition),
	}
	for _, state := range lifecycle.States() {
		description.StateTraits[state] = lifecycle.StateTraits(state)
		description.Terminal[state] = lifecycle.Terminal(state)
		description.Dispositions[state] = lifecycle.Disposition(state)
	}
	for _, edge := range lifecycle.Edges() {
		description.EdgeTraits[[2]string{edge.From, edge.Event}] = lifecycle.EdgeTraits(edge.From, edge.Event)
	}
	return description
}

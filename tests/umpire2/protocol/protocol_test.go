package protocol

import (
	"context"
	"errors"
	"iter"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
)

type RegistrationEntityA struct{}

func (*RegistrationEntityA) Type() umpire.EntityType { return "RegistrationEntityA" }
func (*RegistrationEntityA) OnFact(context.Context, *umpire.EntityPath, iter.Seq[umpire.Fact]) error {
	return nil
}

type RegistrationEntityB struct{}

func (*RegistrationEntityB) Type() umpire.EntityType { return "RegistrationEntityB" }
func (*RegistrationEntityB) OnFact(context.Context, *umpire.EntityPath, iter.Seq[umpire.Fact]) error {
	return nil
}

type RegistrationFact struct {
	calls *[]string
}

func (f *RegistrationFact) Name() string {
	*f.calls = append(*f.calls, "fact")
	return "RegistrationFact"
}

func (*RegistrationFact) TargetEntity() *umpire.EntityPath { return nil }

type compilerRealizer struct{}

func (compilerRealizer) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (compilerRealizer) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return nil
}

func TestProtocolLifecycleReturnsFreshInstances(t *testing.T) {
	protocol, err := Compile(activeDeclaration())
	require.NoError(t, err)

	first, ok := protocol.Lifecycle("CompilerLifecycleEntity")
	require.True(t, ok)
	second, ok := protocol.Lifecycle("CompilerLifecycleEntity")
	require.True(t, ok)

	require.NotSame(t, first, second)
	first.SetState("done")
	require.Equal(t, "created", second.Current())
}

func TestProtocolActionDefensivelyCopiesDeclaration(t *testing.T) {
	declaration := activeDeclaration()
	key := declaration.Entities[0].Actions[0].Key
	declaration.Entities[0].Actions[0].Action.Requires = []umpire.Pre{
		{Ref: umpire.Ref{Type: "CompilerLifecycleEntity"}, State: "created"},
	}
	declaration.Entities[0].Actions[0].Action.Entry = []string{"entry"}
	declaration.Entities[0].Actions[0].Action.Footprint = []string{"footprint"}
	declaration.Entities[0].Actions[0].Action.Reject = &umpire.Reject{Code: "InvalidArgument"}
	declaration.Entities[0].Actions[0].Action.Realize = compilerRealizer{}

	protocol, err := Compile(declaration)
	require.NoError(t, err)

	declaration.Entities[0].Actions[0].Action.Requires[0].State = "mutated"
	declaration.Entities[0].Actions[0].Action.Effects[0].Event = "mutated"
	declaration.Entities[0].Actions[0].Action.Entry[0] = "mutated"
	declaration.Entities[0].Actions[0].Action.Footprint[0] = "mutated"
	declaration.Entities[0].Actions[0].Action.Reject.Code = "mutated"

	action, ok := protocol.Action(key)
	require.True(t, ok)
	require.Equal(t, "created", action.Requires[0].State)
	require.Equal(t, "finish", action.Effects[0].Event)
	require.Equal(t, "entry", action.Entry[0])
	require.Equal(t, "footprint", action.Footprint[0])
	require.Equal(t, "InvalidArgument", action.Reject.Code)
	require.IsType(t, compilerRealizer{}, action.Realize)

	action.Requires[0].State = "changed after read"
	action.Effects[0].Event = "changed after read"
	action.Entry[0] = "changed after read"
	action.Footprint[0] = "changed after read"
	action.Reject.Code = "changed after read"

	again, ok := protocol.Action(key)
	require.True(t, ok)
	require.Equal(t, "created", again.Requires[0].State)
	require.Equal(t, "finish", again.Effects[0].Event)
	require.Equal(t, "entry", again.Entry[0])
	require.Equal(t, "footprint", again.Footprint[0])
	require.Equal(t, "InvalidArgument", again.Reject.Code)
	require.IsType(t, compilerRealizer{}, again.Realize)
}

func TestProtocolRelationsAreValidatedAndDefensivelyCopied(t *testing.T) {
	declaration := activeDeclaration()
	declaration.Relations = []umpire.RelationSchema{{
		Type:              "compiler-parent",
		Source:            "CompilerLifecycleEntity",
		Target:            "CompilerLifecycleEntity",
		SourceCardinality: umpire.RelationMany,
		TargetCardinality: umpire.RelationOne,
	}}
	protocol, err := Compile(declaration)
	require.NoError(t, err)

	declaration.Relations[0].Target = "mutated"
	schemas := protocol.RelationSchemas()
	require.Equal(t, []umpire.RelationSchema{{
		Type:              "compiler-parent",
		Source:            "CompilerLifecycleEntity",
		Target:            "CompilerLifecycleEntity",
		SourceCardinality: umpire.RelationMany,
		TargetCardinality: umpire.RelationOne,
	}}, schemas)
	schemas[0].Target = "changed after read"
	require.Equal(t, umpire.EntityType("CompilerLifecycleEntity"), protocol.RelationSchemas()[0].Target)

	store, err := protocol.NewRelationStore()
	require.NoError(t, err)
	added, err := store.Add(umpire.RelationEdge{
		Type:   "compiler-parent",
		Source: umpire.NewEntityID("CompilerLifecycleEntity", "parent"),
		Target: umpire.NewEntityID("CompilerLifecycleEntity", "child"),
	})
	require.NoError(t, err)
	require.True(t, added)
}

func TestProtocolRelationsRejectUnknownEndpoints(t *testing.T) {
	declaration := activeDeclaration()
	declaration.Relations = []umpire.RelationSchema{{
		Type:   "compiler-missing",
		Source: "CompilerLifecycleEntity",
		Target: "MissingEntity",
	}}

	protocol, err := Compile(declaration)

	require.Nil(t, protocol)
	require.ErrorContains(t, err, `relation "compiler-missing" targets unknown entity "MissingEntity"`)
}

func TestProtocolRegisterCreatesEntitiesFromDeclaredFacts(t *testing.T) {
	target := &umpire.EntityPath{EntityID: umpire.NewEntityID("CompilerEntity", "entity-id")}
	fact := &CompilerFact{target: target}
	protocol, err := Compile(Declaration{
		Facts: []umpire.Fact{fact},
		Entities: []EntityDeclaration{
			{Type: "CompilerEntity", New: compilerEntityFactory, Facts: []umpire.Fact{fact}},
		},
	})
	require.NoError(t, err)
	modelState := umpire.NewModelState()

	protocol.Register(modelState)
	require.NoError(t, modelState.RouteFacts(context.Background(), []umpire.Fact{fact}))

	require.Len(t, modelState.QueryEntities("CompilerEntity", 0, nil), 1)
}

func TestProtocolRegisterPreservesDeclarationOrder(t *testing.T) {
	var calls []string
	fact := &RegistrationFact{calls: &calls}
	protocol, err := Compile(Declaration{
		Facts: []umpire.Fact{fact},
		Entities: []EntityDeclaration{
			{
				Type: "RegistrationEntityA",
				New: func() umpire.Entity {
					calls = append(calls, "a")
					return &RegistrationEntityA{}
				},
				Facts: []umpire.Fact{fact},
			},
			{
				Type: "RegistrationEntityB",
				New: func() umpire.Entity {
					calls = append(calls, "b")
					return &RegistrationEntityB{}
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []umpire.EntityType{"RegistrationEntityA", "RegistrationEntityB"}, protocol.entityOrder)
	calls = nil

	protocol.Register(umpire.NewModelState())

	require.Equal(t, []string{"fact", "a", "b"}, calls)
}

func TestProtocolPlanToDelegatesToLifecyclePlanner(t *testing.T) {
	protocol, err := Compile(activeDeclaration())
	require.NoError(t, err)

	plan, err := protocol.PlanTo("CompilerLifecycleEntity", "done", umpire.Shortest, umpire.Constraints{})

	require.NoError(t, err)
	require.Equal(t, [][]string{{"finish"}}, plan.Routes)
}

func TestProtocolPlanToPreservesPlannerConstraints(t *testing.T) {
	protocol, err := Compile(activeDeclaration())
	require.NoError(t, err)

	_, err = protocol.PlanTo(
		"CompilerLifecycleEntity",
		"done",
		umpire.Shortest,
		umpire.Constraints{DenyEvents: []string{"finish"}},
	)

	require.ErrorContains(t, err, "unreachable under the given constraints")
}

func TestProtocolPlanToForwardsPlannerOptions(t *testing.T) {
	newLifecycle := func() *umpire.Lifecycle {
		return umpire.NewLifecycle(umpire.LifecycleSpec{
			Initial: "created",
			States:  umpire.States{"created": {}, "left": {}, "right": {}, "done": {}},
			Transitions: []umpire.Transition{
				{Event: "go-left", From: []string{"created"}, To: "left"},
				{Event: "go-right", From: []string{"created"}, To: "right"},
				{Event: "finish-left", From: []string{"left"}, To: "done"},
				{Event: "finish-right", From: []string{"right"}, To: "done"},
			},
		})
	}
	protocol, err := Compile(Declaration{
		Entities: []EntityDeclaration{
			{
				Type: "CompilerLifecycleEntity",
				New: func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: newLifecycle()}
				},
			},
		},
	})
	require.NoError(t, err)

	var routes [][][]string
	for _, seed := range []int64{1, 2} {
		want, err := umpire.PlanTo(
			newLifecycle(), "done", umpire.Random, umpire.Constraints{}, umpire.WithSeed(seed),
		)
		require.NoError(t, err)
		got, err := protocol.PlanTo(
			"CompilerLifecycleEntity",
			"done",
			umpire.Random,
			umpire.Constraints{},
			umpire.WithSeed(seed),
		)
		require.NoError(t, err)
		require.Equal(t, want, got)
		routes = append(routes, got.Routes)
	}
	require.NotEqual(t, routes[0], routes[1])
}

func TestProtocolPlanToRejectsUnknownAndMonitorOnlyEntities(t *testing.T) {
	active, err := Compile(activeDeclaration())
	require.NoError(t, err)
	monitorOnly, err := Compile(Declaration{
		Facts: []umpire.Fact{&CompilerFact{}},
		Entities: []EntityDeclaration{
			{Type: "CompilerEntity", New: compilerEntityFactory, Facts: []umpire.Fact{&CompilerFact{}}},
		},
	})
	require.NoError(t, err)

	_, err = active.PlanTo("MissingEntity", "done", umpire.Shortest, umpire.Constraints{})
	require.ErrorContains(t, err, "unknown entity")
	_, err = monitorOnly.PlanTo("CompilerEntity", "done", umpire.Shortest, umpire.Constraints{})
	require.ErrorContains(t, err, "not lifecycled")
}

func TestDefaultProtocolSupportsConcurrentReads(t *testing.T) {
	protocol, err := Default()
	require.NoError(t, err)

	const workers = 32
	errs := make(chan error, workers)
	var waitGroup sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			hosting := umpire.Standalone
			if worker%2 == 1 {
				hosting = umpire.Embedded
			}
			if _, err := protocol.PlanEdge(
				model.NexusOperationType,
				model.NexusStarted,
				model.NexusSucceed,
				hosting,
			); err != nil {
				errs <- err
				return
			}
			lifecycle, ok := protocol.Lifecycle(model.WorkflowType)
			if !ok || lifecycle == nil {
				errs <- errors.New("workflow lifecycle unavailable")
				return
			}
			protocol.Register(umpire.NewModelState())
			errs <- nil
		}(worker)
	}
	waitGroup.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

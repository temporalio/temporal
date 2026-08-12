package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

func plannerDeclaration() Declaration {
	fact := &CompilerFact{}
	lifecycle := func() *umpire.Lifecycle {
		return umpire.NewLifecycle(umpire.LifecycleSpec{
			Initial: "created",
			States:  umpire.States{"created": {}, "started": {}, "done": {}},
			Transitions: []umpire.Transition{
				{Event: "start", From: []string{"created"}, To: "started"},
				{Event: "finish", From: []string{"started"}, To: "done"},
			},
		})
	}
	binding := func(from, event string, hosting umpire.Hosting, name string) ActionBinding {
		return ActionBinding{
			Key: ActionKey{
				Entity:  "CompilerLifecycleEntity",
				From:    from,
				Event:   event,
				Hosting: hosting,
			},
			Action: umpire.Action{
				Name:    name,
				Hosting: hosting,
				Effects: []umpire.Effect{
					{Ref: umpire.Ref{Type: "CompilerLifecycleEntity"}, Event: event},
				},
			},
		}
	}
	return Declaration{
		Facts: []umpire.Fact{fact},
		Entities: []EntityDeclaration{
			{
				Type: "CompilerLifecycleEntity",
				New: func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: lifecycle()}
				},
				Facts: []umpire.Fact{fact},
				Actions: []ActionBinding{
					binding("created", "start", umpire.Standalone, "start-standalone"),
					binding("started", "finish", umpire.Standalone, "finish-standalone"),
					binding("created", "start", umpire.Embedded, "start-embedded"),
					binding("started", "finish", umpire.Embedded, "finish-embedded"),
				},
			},
		},
	}
}

func actionNames(actions []umpire.Action) []string {
	names := make([]string, len(actions))
	for i, action := range actions {
		names[i] = action.Name
	}
	return names
}

func TestProtocolPlanEdgeAssemblesRouteWithExactHosting(t *testing.T) {
	protocol, err := Compile(plannerDeclaration())
	require.NoError(t, err)

	standalone, err := protocol.PlanEdge(
		"CompilerLifecycleEntity", "started", "finish", umpire.Standalone,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"start-standalone", "finish-standalone"}, actionNames(standalone))

	embedded, err := protocol.PlanEdge(
		"CompilerLifecycleEntity", "started", "finish", umpire.Embedded,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"start-embedded", "finish-embedded"}, actionNames(embedded))
}

func TestProtocolPlanEdgeReportsExplicitGap(t *testing.T) {
	declaration := plannerDeclaration()
	entity := &declaration.Entities[0]
	key := entity.Actions[3].Key
	entity.Actions = entity.Actions[:3]
	entity.ActionGaps = []ActionGap{{Key: key, Reason: "needs a real timer"}}
	protocol, err := Compile(declaration)
	require.NoError(t, err)

	_, err = protocol.PlanEdge(
		"CompilerLifecycleEntity", "started", "finish", umpire.Embedded,
	)

	require.ErrorContains(t, err, "CompilerLifecycleEntity:started --finish--> under Embedded")
	require.ErrorContains(t, err, "needs a real timer")
}

func TestProtocolPlanEdgeReportsMissingAction(t *testing.T) {
	declaration := plannerDeclaration()
	declaration.Entities[0].Actions = declaration.Entities[0].Actions[:1]
	protocol, err := Compile(declaration)
	require.NoError(t, err)

	_, err = protocol.PlanEdge(
		"CompilerLifecycleEntity", "started", "finish", umpire.Standalone,
	)

	require.ErrorContains(t, err, "CompilerLifecycleEntity:started --finish--> under Standalone")
	require.ErrorContains(t, err, "no action")
}

func TestProtocolPlanEdgeReportsContextualErrors(t *testing.T) {
	protocol, err := Compile(plannerDeclaration())
	require.NoError(t, err)
	monitorOnly, err := Compile(Declaration{
		Entities: []EntityDeclaration{{Type: "CompilerEntity", New: compilerEntityFactory}},
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		protocol   *Protocol
		entityType umpire.EntityType
		from       string
		event      string
		hosting    umpire.Hosting
		wantError  string
	}{
		{
			name:       "unknown entity",
			protocol:   protocol,
			entityType: "MissingEntity",
			from:       "started",
			event:      "finish",
			hosting:    umpire.Standalone,
			wantError:  "unknown entity",
		},
		{
			name:       "monitor-only entity",
			protocol:   monitorOnly,
			entityType: "CompilerEntity",
			from:       "started",
			event:      "finish",
			hosting:    umpire.Standalone,
			wantError:  "not lifecycled",
		},
		{
			name:       "unknown edge",
			protocol:   protocol,
			entityType: "CompilerLifecycleEntity",
			from:       "started",
			event:      "missing",
			hosting:    umpire.Standalone,
			wantError:  "unknown edge",
		},
		{
			name:       "wildcard hosting",
			protocol:   protocol,
			entityType: "CompilerLifecycleEntity",
			from:       "started",
			event:      "finish",
			hosting:    umpire.AnyHosting,
			wantError:  "concrete hosting",
		},
		{
			name:       "invalid hosting",
			protocol:   protocol,
			entityType: "CompilerLifecycleEntity",
			from:       "started",
			event:      "finish",
			hosting:    umpire.Hosting(99),
			wantError:  "Standalone or Embedded",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.protocol.PlanEdge(
				test.entityType,
				test.from,
				test.event,
				test.hosting,
			)

			require.ErrorContains(t, err, test.wantError)
			require.ErrorContains(t, err, string(test.entityType))
			require.ErrorContains(t, err, test.from)
			require.ErrorContains(t, err, test.event)
			require.ErrorContains(t, err, test.hosting.String())
		})
	}
}

func TestProtocolPlanEdgePropagatesRoutePlannerFailure(t *testing.T) {
	lifecycle := func() *umpire.Lifecycle {
		return umpire.NewLifecycle(umpire.LifecycleSpec{
			Initial: "created",
			States:  umpire.States{"created": {}, "embedded": {}, "done": {}},
			Transitions: []umpire.Transition{
				{
					Event:  "enter",
					From:   []string{"created"},
					To:     "embedded",
					Traits: umpire.Traits{umpire.RequiresHosting(umpire.Embedded)},
				},
				{Event: "finish", From: []string{"embedded"}, To: "done"},
			},
		})
	}
	protocol, err := Compile(Declaration{
		Entities: []EntityDeclaration{
			{
				Type: "CompilerLifecycleEntity",
				New: func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: lifecycle()}
				},
				Actions: []ActionBinding{
					{
						Key: ActionKey{
							Entity:  "CompilerLifecycleEntity",
							From:    "embedded",
							Event:   "finish",
							Hosting: umpire.Standalone,
						},
						Action: umpire.Action{
							Name:    "finish",
							Hosting: umpire.Standalone,
							Effects: []umpire.Effect{{
								Ref:   umpire.Ref{Type: "CompilerLifecycleEntity"},
								Event: "finish",
							}},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = protocol.PlanEdge(
		"CompilerLifecycleEntity", "embedded", "finish", umpire.Standalone,
	)

	require.ErrorContains(t, err, "CompilerLifecycleEntity:embedded --finish--> under Standalone")
	require.ErrorContains(t, err, "route needs Embedded hosting")
}

func TestProtocolPlanEdgeAdvancesRepeatedEventsByExactEdge(t *testing.T) {
	lifecycle := func() *umpire.Lifecycle {
		return umpire.NewLifecycle(umpire.LifecycleSpec{
			Initial: "created",
			States:  umpire.States{"created": {}, "middle": {}, "done": {}},
			Transitions: []umpire.Transition{
				{Event: "advance", From: []string{"created"}, To: "middle"},
				{Event: "advance", From: []string{"middle"}, To: "done"},
			},
		})
	}
	binding := func(from, name string) ActionBinding {
		return ActionBinding{
			Key: ActionKey{
				Entity:  "CompilerLifecycleEntity",
				From:    from,
				Event:   "advance",
				Hosting: umpire.Standalone,
			},
			Action: umpire.Action{
				Name:    name,
				Hosting: umpire.Standalone,
				Effects: []umpire.Effect{{
					Ref:   umpire.Ref{Type: "CompilerLifecycleEntity"},
					Event: "advance",
				}},
			},
		}
	}
	declaration := Declaration{
		Entities: []EntityDeclaration{
			{
				Type: "CompilerLifecycleEntity",
				New: func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: lifecycle()}
				},
				Actions: []ActionBinding{
					binding("created", "first"),
					binding("middle", "second"),
				},
			},
		},
	}
	protocol, err := Compile(declaration)
	require.NoError(t, err)

	firstDestination, ok := lifecycleEdgeDestination(lifecycle(), "created", "advance")
	require.True(t, ok)
	require.Equal(t, "middle", firstDestination)
	secondDestination, ok := lifecycleEdgeDestination(lifecycle(), "middle", "advance")
	require.True(t, ok)
	require.Equal(t, "done", secondDestination)

	actions, err := protocol.PlanEdge(
		"CompilerLifecycleEntity", "middle", "advance", umpire.Standalone,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, actionNames(actions))
}

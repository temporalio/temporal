package protocol

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

type CompilerFact struct {
	target *umpire.EntityPath
}

func (*CompilerFact) Name() string { return "CompilerFact" }
func (f *CompilerFact) TargetEntity() *umpire.EntityPath {
	return f.target
}

type CompilerEntity struct{}

func (*CompilerEntity) Type() umpire.EntityType { return "CompilerEntity" }
func (*CompilerEntity) OnFact(context.Context, *umpire.EntityPath, iter.Seq[umpire.Fact]) error {
	return nil
}

func compilerEntityFactory() umpire.Entity {
	return &CompilerEntity{}
}

type CompilerLifecycleEntity struct {
	lifecycle *umpire.Lifecycle
}

func (*CompilerLifecycleEntity) Type() umpire.EntityType { return "CompilerLifecycleEntity" }
func (*CompilerLifecycleEntity) OnFact(context.Context, *umpire.EntityPath, iter.Seq[umpire.Fact]) error {
	return nil
}
func (e *CompilerLifecycleEntity) Lifecycle() *umpire.Lifecycle { return e.lifecycle }

type MisnamedFact struct{}

func (*MisnamedFact) Name() string                     { return "not-MisnamedFact" }
func (*MisnamedFact) TargetEntity() *umpire.EntityPath { return nil }

type MisnamedEntity struct{}

func (*MisnamedEntity) Type() umpire.EntityType { return "DifferentEntityName" }
func (*MisnamedEntity) OnFact(context.Context, *umpire.EntityPath, iter.Seq[umpire.Fact]) error {
	return nil
}

func compilerLifecycle(hosting umpire.Hosting) *umpire.Lifecycle {
	var traits umpire.Traits
	if hosting != umpire.AnyHosting {
		traits = umpire.Traits{umpire.RequiresHosting(hosting)}
	}
	return umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "created",
		States:  umpire.States{"created": {}, "done": {}},
		Transitions: []umpire.Transition{
			{Event: "finish", From: []string{"created"}, To: "done", Traits: traits},
		},
	})
}

func activeDeclaration() Declaration {
	fact := &CompilerFact{}
	return Declaration{
		Facts: []umpire.Fact{fact},
		Entities: []EntityDeclaration{
			{
				Type: "CompilerLifecycleEntity",
				New: func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: compilerLifecycle(umpire.AnyHosting)}
				},
				Facts: []umpire.Fact{fact},
				Actions: []ActionBinding{
					{
						Key: ActionKey{
							Entity:  "CompilerLifecycleEntity",
							From:    "created",
							Event:   "finish",
							Hosting: umpire.Standalone,
						},
						Action: umpire.Action{
							Name:    "finish",
							Hosting: umpire.Standalone,
							Effects: []umpire.Effect{
								{Ref: umpire.Ref{Type: "CompilerLifecycleEntity"}, Event: "finish"},
							},
						},
					},
				},
			},
		},
	}
}

func TestCompileAcceptsValidMonitorOnlyDeclaration(t *testing.T) {
	fact := &CompilerFact{}

	protocol, err := Compile(Declaration{
		Facts: []umpire.Fact{fact},
		Entities: []EntityDeclaration{
			{
				Type:  "CompilerEntity",
				New:   compilerEntityFactory,
				Facts: []umpire.Fact{fact},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, protocol)
}

func TestCompileAcceptsValidActiveDeclaration(t *testing.T) {
	protocol, err := Compile(activeDeclaration())

	require.NoError(t, err)
	require.NotNil(t, protocol)
}

func TestCompileRejectsInvalidDeclarations(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*Declaration)
		wantError string
	}{
		{
			name: "duplicate fact type",
			mutate: func(d *Declaration) {
				d.Facts = append(d.Facts, &CompilerFact{})
			},
			wantError: "duplicate fact",
		},
		{
			name: "nil fact",
			mutate: func(d *Declaration) {
				d.Facts = append(d.Facts, nil)
			},
			wantError: "nil fact",
		},
		{
			name: "fact name mismatch",
			mutate: func(d *Declaration) {
				d.Facts = append(d.Facts, &MisnamedFact{})
			},
			wantError: "fact name",
		},
		{
			name: "duplicate entity type",
			mutate: func(d *Declaration) {
				d.Entities = append(d.Entities, d.Entities[0])
			},
			wantError: "duplicate entity",
		},
		{
			name: "nil entity factory",
			mutate: func(d *Declaration) {
				d.Entities[0].New = nil
			},
			wantError: "nil factory",
		},
		{
			name: "nil entity",
			mutate: func(d *Declaration) {
				d.Entities[0].New = func() umpire.Entity { return nil }
			},
			wantError: "nil entity",
		},
		{
			name: "declared entity type mismatch",
			mutate: func(d *Declaration) {
				d.Entities[0].Type = "OtherEntity"
			},
			wantError: "declares type",
		},
		{
			name: "concrete entity type mismatch",
			mutate: func(d *Declaration) {
				d.Entities[0].Type = "DifferentEntityName"
				d.Entities[0].New = func() umpire.Entity { return &MisnamedEntity{} }
				d.Entities[0].Actions = nil
			},
			wantError: "concrete type",
		},
		{
			name: "subscription absent from fact set",
			mutate: func(d *Declaration) {
				d.Entities[0].Facts = append(d.Entities[0].Facts, &MisnamedFact{})
			},
			wantError: "subscription",
		},
		{
			name: "invalid lifecycle",
			mutate: func(d *Declaration) {
				d.Entities[0].New = func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: umpire.NewLifecycle(umpire.LifecycleSpec{})}
				}
			},
			wantError: "lifecycle",
		},
		{
			name: "action references another entity",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Key.Entity = "OtherEntity"
			},
			wantError: "action entity",
		},
		{
			name: "action on monitor-only entity",
			mutate: func(d *Declaration) {
				d.Entities[0].New = compilerEntityFactory
				d.Entities[0].Type = "CompilerEntity"
				d.Entities[0].Actions[0].Key.Entity = "CompilerEntity"
				d.Entities[0].Actions[0].Action.Effects[0].Ref.Type = "CompilerEntity"
			},
			wantError: "not lifecycled",
		},
		{
			name: "unknown lifecycle edge",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Key.Event = "missing"
				d.Entities[0].Actions[0].Action.Effects[0].Event = "missing"
			},
			wantError: "unknown edge",
		},
		{
			name: "duplicate action key",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = append(d.Entities[0].Actions, d.Entities[0].Actions[0])
			},
			wantError: "duplicate action",
		},
		{
			name: "wildcard action hosting",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Key.Hosting = umpire.AnyHosting
				d.Entities[0].Actions[0].Action.Hosting = umpire.AnyHosting
			},
			wantError: "concrete hosting",
		},
		{
			name: "invalid action hosting",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Key.Hosting = umpire.Hosting(99)
				d.Entities[0].Actions[0].Action.Hosting = umpire.AnyHosting
			},
			wantError: "Standalone or Embedded",
		},
		{
			name: "action and gap overlap",
			mutate: func(d *Declaration) {
				d.Entities[0].ActionGaps = []ActionGap{{Key: d.Entities[0].Actions[0].Key, Reason: "unsupported"}}
			},
			wantError: "overlaps action",
		},
		{
			name: "gap references another entity",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{
					Key: ActionKey{
						Entity:  "OtherEntity",
						From:    "created",
						Event:   "finish",
						Hosting: umpire.Standalone,
					},
					Reason: "unsupported",
				}}
			},
			wantError: "gap entity",
		},
		{
			name: "gap on monitor-only entity",
			mutate: func(d *Declaration) {
				d.Entities[0].New = compilerEntityFactory
				d.Entities[0].Type = "CompilerEntity"
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{
					Key: ActionKey{
						Entity:  "CompilerEntity",
						From:    "created",
						Event:   "finish",
						Hosting: umpire.Standalone,
					},
					Reason: "unsupported",
				}}
			},
			wantError: "not lifecycled",
		},
		{
			name: "gap references unknown lifecycle edge",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{
					Key: ActionKey{
						Entity:  "CompilerLifecycleEntity",
						From:    "created",
						Event:   "missing",
						Hosting: umpire.Standalone,
					},
					Reason: "unsupported",
				}}
			},
			wantError: "unknown edge",
		},
		{
			name: "duplicate gap key",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				gap := ActionGap{
					Key: ActionKey{
						Entity:  "CompilerLifecycleEntity",
						From:    "created",
						Event:   "finish",
						Hosting: umpire.Standalone,
					},
					Reason: "unsupported",
				}
				d.Entities[0].ActionGaps = []ActionGap{gap, gap}
			},
			wantError: "duplicate gap",
		},
		{
			name: "wildcard gap hosting",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{
					Key: ActionKey{
						Entity: "CompilerLifecycleEntity",
						From:   "created",
						Event:  "finish",
					},
					Reason: "unsupported",
				}}
			},
			wantError: "concrete hosting",
		},
		{
			name: "invalid gap hosting",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{
					Key: ActionKey{
						Entity:  "CompilerLifecycleEntity",
						From:    "created",
						Event:   "finish",
						Hosting: umpire.Hosting(99),
					},
					Reason: "unsupported",
				}}
			},
			wantError: "Standalone or Embedded",
		},
		{
			name: "edge hosting mismatch",
			mutate: func(d *Declaration) {
				d.Entities[0].New = func() umpire.Entity {
					return &CompilerLifecycleEntity{lifecycle: compilerLifecycle(umpire.Standalone)}
				}
				d.Entities[0].Actions[0].Key.Hosting = umpire.Embedded
				d.Entities[0].Actions[0].Action.Hosting = umpire.Embedded
			},
			wantError: "edge hosting",
		},
		{
			name: "action hosting mismatch",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Action.Hosting = umpire.Embedded
			},
			wantError: "action hosting",
		},
		{
			name: "missing matching effect",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions[0].Action.Effects[0].Event = "other"
			},
			wantError: "matching effect",
		},
		{
			name: "gap without reason",
			mutate: func(d *Declaration) {
				d.Entities[0].Actions = nil
				d.Entities[0].ActionGaps = []ActionGap{{Key: ActionKey{
					Entity:  "CompilerLifecycleEntity",
					From:    "created",
					Event:   "finish",
					Hosting: umpire.Standalone,
				}}}
			},
			wantError: "gap reason",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			declaration := activeDeclaration()
			test.mutate(&declaration)

			_, err := Compile(declaration)

			require.ErrorContains(t, err, test.wantError)
		})
	}
}

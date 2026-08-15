package protocol

import (
	"fmt"
	"math/rand"

	"go.temporal.io/server/common/testing/umpire"
)

// SettlingActionPlan is one protocol-derived executable route to a terminal lifecycle edge.
type SettlingActionPlan struct {
	Entity  umpire.EntityType
	From    string
	Event   string
	Hosting umpire.Hosting
	Actions []umpire.Action
}

// SettlingActionPlans retains executable terminal routes and the declared action gaps omitted.
type SettlingActionPlans struct {
	Plans   []SettlingActionPlan
	Omitted []ActionGap
}

// Lifecycle returns a fresh lifecycle for the requested entity type.
func (p *Protocol) Lifecycle(entityType umpire.EntityType) (*umpire.Lifecycle, bool) {
	entity, ok := p.entities[entityType]
	if !ok {
		return nil, false
	}
	probe, err := callEntityFactory(entity.new)
	if err != nil {
		return nil, false
	}
	lifecycled, ok := probe.(umpire.Lifecycled)
	if !ok {
		return nil, false
	}
	return lifecycled.Lifecycle(), true
}

// Action returns a defensive copy of the action bound to key.
func (p *Protocol) Action(key ActionKey) (umpire.Action, bool) {
	action, ok := p.actions[key]
	if !ok {
		return umpire.Action{}, false
	}
	return cloneAction(action), true
}

// PlanTo plans a structural lifecycle route for an entity.
func (p *Protocol) PlanTo(
	entityType umpire.EntityType,
	target string,
	mode umpire.RouteMode,
	constraints umpire.Constraints,
	options ...umpire.Option,
) (*umpire.Plan, error) {
	if _, ok := p.entities[entityType]; !ok {
		return nil, fmt.Errorf("protocol: unknown entity %q", entityType)
	}
	lifecycle, ok := p.Lifecycle(entityType)
	if !ok {
		return nil, fmt.Errorf("protocol: entity %q is not lifecycled", entityType)
	}
	plan, err := umpire.PlanTo(lifecycle, target, mode, constraints, options...)
	if err != nil {
		return nil, fmt.Errorf("protocol: plan %q to %q: %w", entityType, target, err)
	}
	return plan, nil
}

// PlanEdge assembles the executable actions that reach and traverse one lifecycle edge.
func (p *Protocol) PlanEdge(
	entityType umpire.EntityType,
	from string,
	event string,
	hosting umpire.Hosting,
) ([]umpire.Action, error) {
	if _, ok := p.entities[entityType]; !ok {
		return nil, fmt.Errorf(
			"protocol: unknown entity for edge %s",
			actionContext(entityType, from, event, hosting),
		)
	}
	if !isConcreteHosting(hosting) {
		return nil, fmt.Errorf(
			"protocol: edge %s requires concrete hosting: Standalone or Embedded",
			actionContext(entityType, from, event, hosting),
		)
	}
	lifecycle, ok := p.Lifecycle(entityType)
	if !ok {
		return nil, fmt.Errorf(
			"protocol: entity is not lifecycled for edge %s",
			actionContext(entityType, from, event, hosting),
		)
	}
	if !lifecycleHasEdge(lifecycle, from, event) {
		return nil, fmt.Errorf("protocol: unknown edge %s", actionContext(entityType, from, event, hosting))
	}
	if edgeHosting := lifecycle.EdgeHosting(from, event); edgeHosting != umpire.AnyHosting && edgeHosting != hosting {
		return nil, fmt.Errorf(
			"protocol: edge %s is %s-only",
			actionContext(entityType, from, event, hosting),
			edgeHosting,
		)
	}

	var route []string
	if from != lifecycle.Initial() {
		plan, err := umpire.PlanTo(
			lifecycle,
			from,
			umpire.Shortest,
			umpire.Constraints{Hosting: hosting},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"protocol: route for edge %s: %w",
				actionContext(entityType, from, event, hosting),
				err,
			)
		}
		route = plan.Routes[0]
	}
	events := append(append([]string(nil), route...), event)
	actions := make([]umpire.Action, 0, len(events))
	state := lifecycle.Initial()
	for _, routeEvent := range events {
		key := ActionKey{Entity: entityType, From: state, Event: routeEvent, Hosting: hosting}
		if reason, gap := p.gaps[key]; gap {
			return nil, fmt.Errorf("protocol: no action for %s: %s", actionContext(entityType, state, routeEvent, hosting), reason)
		}
		action, found := p.Action(key)
		if !found {
			return nil, fmt.Errorf("protocol: no action for %s", actionContext(entityType, state, routeEvent, hosting))
		}
		actions = append(actions, action)
		destination, found := lifecycleEdgeDestination(lifecycle, state, routeEvent)
		if !found {
			return nil, fmt.Errorf(
				"protocol: route contains unknown edge %s",
				actionContext(entityType, state, routeEvent, hosting),
			)
		}
		state = destination
	}
	return actions, nil
}

// PlanSettlingEdges derives one executable plan per drivable terminal edge from the protocol.
func (p *Protocol) PlanSettlingEdges(
	entityType umpire.EntityType,
	defaultHosting umpire.Hosting,
) (SettlingActionPlans, error) {
	lifecycle, ok := p.Lifecycle(entityType)
	if !ok {
		return SettlingActionPlans{}, fmt.Errorf("protocol: entity %q is not lifecycled", entityType)
	}
	result := SettlingActionPlans{}
	for _, edge := range lifecycle.Edges() {
		if !lifecycle.Terminal(edge.To) {
			continue
		}
		hosting := defaultHosting
		if constrained := lifecycle.EdgeHosting(edge.From, edge.Event); constrained != umpire.AnyHosting {
			hosting = constrained
		}
		actions, err := p.PlanEdge(entityType, edge.From, edge.Event, hosting)
		if err != nil {
			key := ActionKey{Entity: entityType, From: edge.From, Event: edge.Event, Hosting: hosting}
			reason, declared := p.gaps[key]
			if !declared {
				reason = err.Error()
			}
			result.Omitted = append(result.Omitted, ActionGap{Key: key, Reason: reason})
			continue
		}
		result.Plans = append(result.Plans, SettlingActionPlan{
			Entity: entityType, From: edge.From, Event: edge.Event, Hosting: hosting, Actions: actions,
		})
	}
	return result, nil
}

// SampleSettlingPlan reproducibly selects one protocol-derived executable terminal route.
func (p *Protocol) SampleSettlingPlan(
	entityType umpire.EntityType,
	defaultHosting umpire.Hosting,
	seed int64,
) (SettlingActionPlan, error) {
	plans, err := p.PlanSettlingEdges(entityType, defaultHosting)
	if err != nil {
		return SettlingActionPlan{}, err
	}
	if len(plans.Plans) == 0 {
		return SettlingActionPlan{}, fmt.Errorf("protocol: entity %q has no drivable settling edge", entityType)
	}
	return plans.Plans[rand.New(rand.NewSource(seed)).Intn(len(plans.Plans))], nil
}

func actionContext(entityType umpire.EntityType, from, event string, hosting umpire.Hosting) string {
	return fmt.Sprintf("%s:%s --%s--> under %s", entityType, from, event, hosting)
}

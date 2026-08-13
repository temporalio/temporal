package protocol

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
)

// Compile validates and indexes a protocol declaration.
func Compile(declaration Declaration) (*Protocol, error) {
	protocol := &Protocol{
		facts:       slices.Clone(declaration.Facts),
		entityOrder: make([]umpire.EntityType, 0, len(declaration.Entities)),
		entities:    make(map[umpire.EntityType]compiledEntity, len(declaration.Entities)),
		actions:     make(map[ActionKey]umpire.Action),
		gaps:        make(map[ActionKey]string),
		relations:   slices.Clone(declaration.Relations),
		derivers:    slices.Clone(declaration.RelationDerivers),
		regression:  declaration.Regression.Clone(),
	}
	factTypes := make(map[reflect.Type]struct{}, len(declaration.Facts))
	for i, fact := range declaration.Facts {
		factType, factName, err := declaredFactType(fact)
		if err != nil {
			return nil, fmt.Errorf("protocol: fact %d: %w", i, err)
		}
		if _, exists := factTypes[factType]; exists {
			return nil, fmt.Errorf("protocol: duplicate fact type %s", factType)
		}
		factTypes[factType] = struct{}{}
		if fact.Name() != factName {
			return nil, fmt.Errorf("protocol: fact name %q does not match concrete type %q", fact.Name(), factName)
		}
	}

	for _, entity := range declaration.Entities {
		if _, exists := protocol.entities[entity.Type]; exists {
			return nil, fmt.Errorf("protocol: duplicate entity type %q", entity.Type)
		}
		if entity.New == nil {
			return nil, fmt.Errorf("protocol: entity %q has nil factory", entity.Type)
		}
		probe, err := callEntityFactory(entity.New)
		if err != nil {
			return nil, fmt.Errorf("protocol: entity %q: %w", entity.Type, err)
		}
		if probe.Type() != entity.Type {
			return nil, fmt.Errorf("protocol: entity factory returns type %q but declares type %q", probe.Type(), entity.Type)
		}
		concreteName, err := entityConcreteName(probe)
		if err != nil {
			return nil, fmt.Errorf("protocol: entity %q: %w", entity.Type, err)
		}
		if string(entity.Type) != concreteName {
			return nil, fmt.Errorf("protocol: entity %q does not match concrete type %q", entity.Type, concreteName)
		}
		for _, subscription := range entity.Facts {
			subscriptionType, _, err := declaredFactType(subscription)
			if err != nil {
				return nil, fmt.Errorf("protocol: entity %q subscription: %w", entity.Type, err)
			}
			if _, registered := factTypes[subscriptionType]; !registered {
				return nil, fmt.Errorf("protocol: entity %q subscription %s is absent from the fact set", entity.Type, subscriptionType)
			}
		}

		var lifecycle *umpire.Lifecycle
		if lifecycled, ok := probe.(umpire.Lifecycled); ok {
			lifecycle = lifecycled.Lifecycle()
			if lifecycle == nil {
				return nil, fmt.Errorf("protocol: entity %q has nil lifecycle", entity.Type)
			}
			if err := lifecycle.Validate(); err != nil {
				return nil, fmt.Errorf("protocol: entity %q lifecycle: %w", entity.Type, err)
			}
		}

		protocol.entities[entity.Type] = compiledEntity{
			new:   entity.New,
			facts: slices.Clone(entity.Facts),
		}
		protocol.entityOrder = append(protocol.entityOrder, entity.Type)
		for _, binding := range entity.Actions {
			if err := protocol.addAction(entity.Type, lifecycle, binding); err != nil {
				return nil, err
			}
		}
		for _, gap := range entity.ActionGaps {
			if err := protocol.addGap(entity.Type, lifecycle, gap); err != nil {
				return nil, err
			}
		}
	}
	if _, err := umpire.NewRelationStore(protocol.relations...); err != nil {
		return nil, fmt.Errorf("protocol: relations: %w", err)
	}
	for _, relation := range protocol.relations {
		if _, exists := protocol.entities[relation.Source]; !exists {
			return nil, fmt.Errorf("protocol: relation %q sources unknown entity %q", relation.Type, relation.Source)
		}
		if _, exists := protocol.entities[relation.Target]; !exists {
			return nil, fmt.Errorf("protocol: relation %q targets unknown entity %q", relation.Type, relation.Target)
		}
	}
	for index, derive := range protocol.derivers {
		if derive == nil {
			return nil, fmt.Errorf("protocol: relation deriver %d is nil", index)
		}
	}
	footprints, err := compileCausalFootprints(
		protocol.regression,
		protocol.facts,
		protocol.relations,
		declaration.CausalFootprints,
	)
	if err != nil {
		return nil, err
	}
	protocol.footprints = footprints
	return protocol, nil
}

func declaredFactType(fact umpire.Fact) (reflect.Type, string, error) {
	if fact == nil {
		return nil, "", errors.New("nil fact")
	}
	factType := reflect.TypeOf(fact)
	factValue := reflect.ValueOf(fact)
	if factType.Kind() != reflect.Pointer || factType.Elem().Kind() != reflect.Struct || factValue.IsNil() {
		return nil, "", fmt.Errorf("fact must be a non-nil pointer to a struct, got %T", fact)
	}
	return factType, factType.Elem().Name(), nil
}

func callEntityFactory(factory umpire.EntityFactory) (entity umpire.Entity, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("factory panicked: %v", recovered)
		}
	}()
	entity = factory()
	if entity == nil {
		return nil, errors.New("factory returned nil entity")
	}
	value := reflect.ValueOf(entity)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, errors.New("factory returned nil entity")
	}
	return entity, nil
}

func entityConcreteName(entity umpire.Entity) (string, error) {
	entityType := reflect.TypeOf(entity)
	if entityType.Kind() != reflect.Pointer || entityType.Elem().Kind() != reflect.Struct {
		return "", fmt.Errorf("concrete entity must be a pointer to a struct, got %T", entity)
	}
	return entityType.Elem().Name(), nil
}

func (p *Protocol) addAction(entityType umpire.EntityType, lifecycle *umpire.Lifecycle, binding ActionBinding) error {
	key := binding.Key
	if key.Entity != entityType {
		return fmt.Errorf("protocol: entity %q contains action entity %q", entityType, key.Entity)
	}
	if lifecycle == nil {
		return fmt.Errorf("protocol: action %v belongs to entity %q, which is not lifecycled", key, entityType)
	}
	if !isConcreteHosting(key.Hosting) {
		return fmt.Errorf("protocol: action %v requires concrete hosting: Standalone or Embedded", key)
	}
	if !lifecycleHasEdge(lifecycle, key.From, key.Event) {
		return fmt.Errorf("protocol: action %v references unknown edge", key)
	}
	if _, exists := p.actions[key]; exists {
		return fmt.Errorf("protocol: duplicate action key %v", key)
	}
	if edgeHosting := lifecycle.EdgeHosting(key.From, key.Event); edgeHosting != umpire.AnyHosting && edgeHosting != key.Hosting {
		return fmt.Errorf("protocol: action %v has %s edge hosting", key, edgeHosting)
	}
	if actionHosting := binding.Action.Hosting; actionHosting != umpire.AnyHosting && actionHosting != key.Hosting {
		return fmt.Errorf("protocol: action hosting %s does not match key %v", actionHosting, key)
	}
	if !actionHasEffect(binding.Action, key.Entity, key.Event) {
		return fmt.Errorf("protocol: action %v has no matching effect", key)
	}
	p.actions[key] = cloneAction(binding.Action)
	action := cloneAction(binding.Action)
	p.actionOrder = append(p.actionOrder, ActionCatalogEntry{Key: key, Action: &action})
	return nil
}

func (p *Protocol) addGap(entityType umpire.EntityType, lifecycle *umpire.Lifecycle, gap ActionGap) error {
	key := gap.Key
	if key.Entity != entityType {
		return fmt.Errorf("protocol: entity %q contains gap entity %q", entityType, key.Entity)
	}
	if lifecycle == nil {
		return fmt.Errorf("protocol: gap %v belongs to entity %q, which is not lifecycled", key, entityType)
	}
	if !isConcreteHosting(key.Hosting) {
		return fmt.Errorf("protocol: gap %v requires concrete hosting: Standalone or Embedded", key)
	}
	if !lifecycleHasEdge(lifecycle, key.From, key.Event) {
		return fmt.Errorf("protocol: gap %v references unknown edge", key)
	}
	if strings.TrimSpace(gap.Reason) == "" {
		return fmt.Errorf("protocol: gap reason is empty for %v", key)
	}
	if _, exists := p.actions[key]; exists {
		return fmt.Errorf("protocol: gap %v overlaps action", key)
	}
	if _, exists := p.gaps[key]; exists {
		return fmt.Errorf("protocol: duplicate gap key %v", key)
	}
	p.gaps[key] = gap.Reason
	p.actionOrder = append(p.actionOrder, ActionCatalogEntry{Key: key, GapReason: gap.Reason})
	return nil
}

func lifecycleHasEdge(lifecycle *umpire.Lifecycle, from, event string) bool {
	_, found := lifecycleEdgeDestination(lifecycle, from, event)
	return found
}

func lifecycleEdgeDestination(lifecycle *umpire.Lifecycle, from, event string) (string, bool) {
	for _, edge := range lifecycle.Edges() {
		if edge.From == from && edge.Event == event {
			return edge.To, true
		}
	}
	return "", false
}

func isConcreteHosting(hosting umpire.Hosting) bool {
	return hosting == umpire.Standalone || hosting == umpire.Embedded
}

func actionHasEffect(action umpire.Action, entityType umpire.EntityType, event string) bool {
	for _, effect := range action.Effects {
		if effect.Ref.Type == entityType && effect.Event == event {
			return true
		}
	}
	return false
}

func cloneAction(action umpire.Action) umpire.Action {
	action.Requires = slices.Clone(action.Requires)
	action.Effects = slices.Clone(action.Effects)
	action.Entry = slices.Clone(action.Entry)
	action.Footprint = slices.Clone(action.Footprint)
	if action.Reject != nil {
		reject := *action.Reject
		action.Reject = &reject
	}
	return action
}

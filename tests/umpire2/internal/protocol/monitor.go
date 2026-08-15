package protocol

import "go.temporal.io/server/common/testing/umpire"

// Register installs the compiled fact and entity declarations into a model state.
func (p *Protocol) Register(modelState *umpire.ModelState) {
	modelState.RegisterFact(p.facts...)
	for _, entityType := range p.entityOrder {
		entity := p.entities[entityType]
		modelState.RegisterEntity(entity.new, entity.facts...)
	}
}

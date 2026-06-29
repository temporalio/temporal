package events

import (
	"errors"
	"fmt"
	"sync"
)

// registry tracks event definitions added via NewEventDef and builds a validated catalog.
// globalRegistry is a process-global singleton so that every event definition in the binary
// registers into one catalog.
type registry struct {
	sync.Mutex
	definitions []eventDefinition
}

// Catalog maps event name to its definition. Built once at startup; do not mutate after.
type Catalog map[string]eventDefinition

var (
	globalRegistry        registry
	errEventAlreadyExists = errors.New("event already exists")
)

func (r *registry) register(d eventDefinition) {
	r.Lock()
	defer r.Unlock()
	r.definitions = append(r.definitions, d)
}

func (r *registry) buildCatalog() (Catalog, error) {
	r.Lock()
	defer r.Unlock()

	c := make(Catalog, len(r.definitions))
	for _, d := range r.definitions {
		if original, ok := c[d.name]; ok {
			return nil, fmt.Errorf(
				"%w: event %q already defined with %+v; cannot redefine with %+v",
				errEventAlreadyExists, d.name, original, d,
			)
		}
		c[d.name] = d
	}
	return c, nil
}

// BuildCatalog builds and validates the global event catalog. Call once at server startup;
// a non-nil error means two events share a name and the server should fail fast.
func BuildCatalog() (Catalog, error) {
	return globalRegistry.buildCatalog()
}

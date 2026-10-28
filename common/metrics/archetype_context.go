package metrics

import "context"

type archetypeContextKey struct{}

// WithArchetypeContext attributes persistence work to a registered root component.
// Callers must use registry display names, never business IDs or user input.
func WithArchetypeContext(ctx context.Context, archetype string) context.Context {
	return context.WithValue(ctx, archetypeContextKey{}, ArchetypeTag(archetype).Value)
}

// ArchetypeFromContext returns the registered root component, or _unknown_ for shared work.
func ArchetypeFromContext(ctx context.Context) string {
	if archetype, ok := ctx.Value(archetypeContextKey{}).(string); ok {
		return archetype
	}
	return unknownValue
}

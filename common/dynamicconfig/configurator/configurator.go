package configurator

import (
	"context"

	lib "go.temporal.io/server/common/dynamicconfig/configurator/internal/library"
	"go.temporal.io/server/common/dynamicconfig/configurator/types"
)

// Config and Override are re-exported so callers need only import this package.
type (
	Config[V any]   = types.Config[V]
	Override[V any] = types.Override[V]
	Lookup          = types.Lookup
	Constraints     = types.Constraints
)

// Configurator evaluates constraint expressions to select a configuration value.
//
// V is opaque: the library parses the match expressions, evaluates them against the
// constraints it is given, and returns whichever V won. It never inspects, decodes or
// converts a value, so a caller can use whatever representation suits it — a decoded Go
// value, a pointer into its own storage, a closure. Callers that want values decoded from
// JSON can use JSONConfig.
type Configurator[V any] interface {
	// Load installs cfg under configKey, replacing anything previously loaded there. The
	// match expressions are parsed here, so a malformed one fails now rather than silently
	// never matching at evaluation time.
	Load(configKey string, cfg Config[V]) error

	// Eval returns the value for configKey: the first override whose expression matches,
	// otherwise the default. It errors only if configKey was never loaded, or if evaluating
	// an expression fails.
	Eval(ctx context.Context, configKey string, constraints Lookup) (V, error)

	// ReferencedKeys returns the constraint keys the expressions for configKey can test, and
	// whether configKey is loaded at all. Useful for telling, at load time, whether an entry's
	// value can vary with a given set of constraints.
	ReferencedKeys(configKey string) ([]string, bool)
}

// New constructs a Configurator[V].
func New[V any]() Configurator[V] {
	return lib.New[V]()
}

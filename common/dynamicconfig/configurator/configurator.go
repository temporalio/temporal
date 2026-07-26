package configurator

import (
	"context"

	lib "go.temporal.io/server/common/dynamicconfig/configurator/internal/library"
	"go.temporal.io/server/common/dynamicconfig/configurator/types"
)

// Configurator is a configuration evaluation library for providing config based on constraints.
// T is the type of values returned by Eval; use New[T] to construct a typed instance.
// Config values are unmarshalled as T using encoding/json.
type Configurator[T any] interface {
	LoadKey(configkey string, data []byte) error
	Eval(ctx context.Context, configKey string, constraints types.Lookup) (T, error)

	// ReferencedKeys returns the constraint keys the expressions for configKey can test, and
	// whether configKey is loaded at all. LOCAL ADDITION, see internal/library/impl.go.
	ReferencedKeys(configKey string) ([]string, bool)
}

// New constructs a Configurator[T] that unmarshals config values as T using encoding/json.
func New[T any]() Configurator[T] {
	return lib.New[T]()
}

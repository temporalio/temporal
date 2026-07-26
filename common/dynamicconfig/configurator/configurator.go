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
	Eval(ctx context.Context, configKey string, constraints types.Constraints) (T, error)
}

// New constructs a Configurator[T] that unmarshals config values as T using encoding/json.
func New[T any]() Configurator[T] {
	return lib.New[T]()
}

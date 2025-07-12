//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package chasm

import "context"

type Engine interface {
	NewEntity(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		...TransitionOption,
	) (EntityKey, []byte, error)
	UpdateWithNewEntity(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...TransitionOption,
	) (EntityKey, []byte, error)

	UpdateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component) error,
		...TransitionOption,
	) ([]byte, error)
	ReadComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
		...TransitionOption,
	) error

	PollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) (any, bool, error),
		func(MutableContext, Component, any) error,
		...TransitionOption,
	) ([]byte, error)

	InternalKeyConverter
}

type engineCtxKeyType string

const engineCtxKey engineCtxKeyType = "chasmEngine"

// this will be done by the nexus handler?
// alternatively the engine can be a global variable,
// but not a good practice in fx.
func NewEngineContext(
	ctx context.Context,
	engine Engine,
) context.Context {
	return context.WithValue(ctx, engineCtxKey, engine)
}

func engineFromContext(
	ctx context.Context,
) Engine {
	e, ok := ctx.Value(engineCtxKey).(Engine)
	if !ok {
		return nil
	}
	return e
}

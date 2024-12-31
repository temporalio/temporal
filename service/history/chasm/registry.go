package chasm

type Registry struct{}

func (r *Registry) RegisterLibrary(lib Library) {
	panic("not implemented")
}

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
	// Service()
}

type RegistrableComponent struct {
}

func NewRegistrableComponent[C Component](
	name string,
	opts ...RegistrableComponentOption,
) RegistrableComponent {
	panic("not implemented")
}

type RegistrableComponentOption func(*RegistrableComponent)

func EntityEphemeral() RegistrableComponentOption {
	panic("not implemented")
}

// Is there any use case where we don't want to replicate
// certain instances of a archetype?
func EntitySingleCluster() RegistrableComponentOption {
	panic("not implemented")
}

func EntityShardingFn(
	func(EntityKey) string,
) RegistrableComponentOption {
	panic("not implemented")
}

type RegistrableTask struct{}

func NewRegistrableTask[C any, T any](
	name string,
	handler TaskHandler[C, T],
	// opts ...RegistrableTaskOptions, no options right now
) RegistrableTask {
	panic("not implemented")
}

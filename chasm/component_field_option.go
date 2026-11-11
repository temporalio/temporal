package chasm

type (
	componentFieldOptions struct {
		detached  bool
		ephemeral bool
	}

	ComponentFieldOption func(*componentFieldOptions)
)

func ComponentFieldDetached() ComponentFieldOption {
	return func(o *componentFieldOptions) {
		o.detached = true
	}
}

// ComponentFieldEphemeral marks the component, and everything under it, as ephemeral. Ephemeral components only live
// in memory, for as long as the execution's mutable state is loaded. They are never persisted or replicated, and they
// can't have tasks.
func ComponentFieldEphemeral() ComponentFieldOption {
	return func(o *componentFieldOptions) {
		o.ephemeral = true
	}
}

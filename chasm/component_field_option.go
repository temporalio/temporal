package chasm

type (
	componentFieldOptions struct {
		detached bool
	}

	ComponentFieldOption func(*componentFieldOptions)
)

func ComponentFieldDetached() ComponentFieldOption {
	return func(o *componentFieldOptions) {
		o.detached = true
	}
}

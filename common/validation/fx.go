package validation

import "go.uber.org/fx"

const validatorGroup = `group:"validationRequestValidators"`

func Module(name string, constructors ...any) fx.Option {
	options := []fx.Option{
		fx.Provide(newValidatorRegistryWithValidators),
	}
	for _, constructor := range constructors {
		options = append(options, validatorProvider(constructor))
	}
	return fx.Module(name, options...)
}

type validatorRegistryParams struct {
	fx.In

	Validators []registeredValidator `group:"validationRequestValidators"`
}

func newValidatorRegistryWithValidators(params validatorRegistryParams) (*ValidatorRegistry, error) {
	registry := NewValidatorRegistry()
	for _, validator := range params.Validators {
		if err := validator.RegisterValidator(registry); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func validatorProvider(constructor any) fx.Option {
	return fx.Provide(
		fx.Annotate(
			constructor,
			fx.As(new(registeredValidator)),
			fx.ResultTags(validatorGroup),
		),
	)
}

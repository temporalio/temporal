package tests

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/validation"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
)

type ValidatorsSuite struct {
	parallelsuite.Suite[*ValidatorsSuite]
}

func TestValidatorsSuite(t *testing.T) {
	parallelsuite.Run(t, &ValidatorsSuite{})
}

func (s *ValidatorsSuite) TestRegisteredValidatorsCoverAllFields() {
	var validatorCount int
	testcore.NewEnv(s.T(),
		testcore.WithFxOptions(
			primitives.FrontendService,
			fx.Invoke(func(lc fx.Lifecycle, registry *validation.ValidatorRegistry) {
				lc.Append(fx.StartHook(func(context.Context) error {
					validatorCount = len(validation.RegisteredValidatorsForTesting(registry))
					if validatorCount <= 1 {
						return fmt.Errorf("expected more than 1 registered validator, got %d", validatorCount)
					}
					return s.validateRegisteredValidators(registry)
				}))
			}),
		),
	)

	if validatorCount <= 1 {
		s.T().Fatalf("expected more than 1 registered validator, got %d", validatorCount)
	}
}

func (s *ValidatorsSuite) validateRegisteredValidators(registry *validation.ValidatorRegistry) error {
	if registry == nil {
		return errors.New("validator registry is nil")
	}

	validators := validation.RegisteredValidatorsForTesting(registry)
	if len(validators) == 0 {
		return errors.New("validator registry has no registered validators")
	}

	types := make([]reflect.Type, 0, len(validators))
	for typ := range validators {
		types = append(types, typ)
	}
	sort.Slice(types, func(i, j int) bool {
		return types[i].String() < types[j].String()
	})

	for _, typ := range types {
		if err := s.validateRegisteredValidator(typ, validators[typ]); err != nil {
			return err
		}
	}
	return nil
}

func (s *ValidatorsSuite) validateRegisteredValidator(requestType reflect.Type, validator any) error {
	value := reflect.ValueOf(validator)
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return fmt.Errorf("validator for %s is nil", requestType)
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil
	}

	validatorType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		fieldValue := value.Field(i)
		structField := validatorType.Field(i)
		if structField.PkgPath != "" || fieldValue.Kind() != reflect.Func {
			continue
		}
		if fieldValue.IsNil() {
			return fmt.Errorf("%s.%s validator is not registered", s.requestTypeName(requestType), structField.Name)
		}
	}
	return nil
}

func (s *ValidatorsSuite) requestTypeName(requestType reflect.Type) string {
	if requestType.Kind() == reflect.Pointer {
		requestType = requestType.Elem()
	}
	if requestType.Name() != "" {
		return requestType.Name()
	}
	return requestType.String()
}

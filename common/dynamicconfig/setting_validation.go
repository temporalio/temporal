package dynamicconfig

import "fmt"

type (
	// Validator checks a converted setting value and returns a non-nil error to reject it. Write
	// one for any rule a setting needs and pass it to WithValidation.
	Validator[T any] func(T) error

	// Number is the set of numeric types a validator such as Positive can check.
	Number interface {
		~int | ~int32 | ~int64 | ~float32 | ~float64
	}
)

// WithValidation wraps a converter so that a converted value must also pass every validator.
// Pass the result to one of the New*TypedSettingWithConverter or
// New*TypedSettingWithConstrainedDefault constructors. A rejected value is treated like one that
// cannot be converted: the collection logs a warning and uses the next matching value or the
// default, and the YAML loader reports it as invalid.
func WithValidation[T any](convert func(any) (T, error), validators ...Validator[T]) func(any) (T, error) {
	return func(val any) (T, error) {
		t, err := convert(val)
		if err != nil {
			return t, err
		}
		for _, validate := range validators {
			if err := validate(t); err != nil {
				var zero T
				return zero, err
			}
		}
		return t, nil
	}
}

// Positive rejects values <= 0 (and NaN). Use it for intervals and sizes where zero does not
// mean "disabled" and would instead spin a loop, panic a ticker, or multiply the work done.
func Positive[T Number](v T) error {
	if !(v > 0) {
		return fmt.Errorf("value must be positive, got %v", v)
	}
	return nil
}

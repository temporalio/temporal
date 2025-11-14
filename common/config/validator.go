package config

import (
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
	"gopkg.in/validator.v2"
)

func newValidator() *validator.Validator {
	validate := validator.NewValidator()
	_ = validate.SetValidationFunc("persistence_custom_search_attributes", validatePersistenceCustomSearchAttributes)
	return validate
}

func validatePersistenceCustomSearchAttributes(v any, param string) error {
	st := reflect.ValueOf(v)
	switch st.Kind() {
	case reflect.Map:
		iter := st.MapRange()
		for iter.Next() {
			// key must be a string and a valid search attribute type
			key := iter.Key()
			if key.Kind() != reflect.String {
				return validator.ErrUnsupported
			}
			if enumspb.IndexedValueType_shorthandValue[key.String()] == 0 {
				return validator.ErrInvalid
			}
			// value must an integer and between 0 and 99
			val := iter.Value()
			var num int64
			if val.CanInt() {
				num = val.Int()
			} else if val.CanUint() {
				num = int64(val.Uint())
			} else {
				return validator.ErrUnsupported
			}
			if num < 0 || num > 99 {
				return validator.ErrInvalid
			}
		}

	default:
		return validator.ErrUnsupported
	}
	return nil
}

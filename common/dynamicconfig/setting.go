//go:generate go run ../../cmd/tools/gendynamicconfig

package dynamicconfig

import (
	"reflect"
	"strings"
	"time"
)

type (
	// Precedence is an enum for the search order precedence of a dynamic config setting.
	// E.g., use the global value, check namespace then global, check task queue then
	// namespace then global, etc.
	Precedence int

	// setting is one dynamic config setting. setting should not be used or created directly,
	// but use one of the generated constructors for instantiated Setting types in
	// setting_gen.go, e.g. NewNamespaceBoolSetting.
	// T is the data type of the setting. P is a go type representing the precedence, which is
	// just used to make the types more unique.
	setting[T any, P any] struct {
		key         Key                  // string value of key. case-insensitive.
		def         T                    // default value
		convert     func(any) (T, error) // converter function
		description string               // documentation
	}

	constrainedDefaultSetting[T any, P any] struct {
		key         Key                        // string value of key. case-insensitive.
		cdef        []TypedConstrainedValue[T] // default values
		convert     func(any) (T, error)       // converter function
		description string                     // documentation
	}

	// SettingDoc contains documentation metadata for a dynamic config setting.
	SettingDoc struct {
		Key          string // setting key name
		Type         string // setting value type (e.g., "bool", "int", "duration", "string", "float", "map", "typed")
		Precedence   string // setting precedence (e.g., "Global", "Namespace", "TaskQueue")
		Description  string // human-readable description
		DefaultValue any    // default value (simple value or []TypedConstrainedValue for constrained defaults)
	}

	// GenericSetting is an interface that all instances of Setting implement (by generated
	// code in setting_gen.go). It can be used to refer to settings of any type and deal with
	// them generically..
	GenericSetting interface {
		Key() Key
		Precedence() Precedence
		Validate(v any) error
		Documentation() SettingDoc

		// for internal use:
		dispatchUpdate(*Collection, any, []ConstrainedValue)
	}

	// GenericParseHook is an interface that may be implemented by a setting type or a field
	// contained inside a struct setting type.
	// It should be implemented with a non-pointer receiver that will be ignored, and return
	// the parsed value and any parse error.
	// Type "S" is usually "string", and "T" must be the same as the receiver type.
	GenericParseHook[S, T any] interface {
		DynamicConfigParseHook(S) (T, error)
	}
)

// formatDefaultValue formats a default value for display, converting durations to strings
func formatDefaultValue(v any) any {
	if d, ok := v.(time.Duration); ok {
		return d.String()
	}
	return v
}

// formatConstrainedDefaults formats constrained default values, converting durations to strings
func formatConstrainedDefaults[T any](cdef []TypedConstrainedValue[T]) any {
	// Create a copy with formatted values
	result := make([]TypedConstrainedValue[any], len(cdef))
	for i, cv := range cdef {
		result[i] = TypedConstrainedValue[any]{
			Constraints: cv.Constraints,
			Value:       formatDefaultValue(cv.Value),
		}
	}
	return result
}

// getTypeName returns a human-readable type name for documentation.
func getTypeName(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "any"
	}

	// Handle common types
	switch t.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Special case for time.Duration
		if t.String() == "time.Duration" {
			return "duration"
		}
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.String:
		return "string"
	case reflect.Map:
		keyType := getTypeNameFromType(t.Key())
		valueType := getTypeNameFromType(t.Elem())
		return "map[" + keyType + "]" + valueType
	case reflect.Slice:
		elemType := getTypeNameFromType(t.Elem())
		return "[]" + elemType
	case reflect.Struct:
		// Check for time.Duration and other known structs
		typeName := t.String()
		if strings.Contains(typeName, ".") {
			// Remove package prefix for cleaner names
			parts := strings.Split(typeName, ".")
			return strings.ToLower(parts[len(parts)-1])
		}
		return "struct"
	case reflect.Ptr:
		// For pointer types, get the type of what it points to
		return getTypeNameFromType(t.Elem())
	default:
		return "typed"
	}
}

// getTypeNameFromType returns a type name from a reflect.Type
func getTypeNameFromType(t reflect.Type) string {
	// Handle interface types (like any/interface{})
	if t.Kind() == reflect.Interface {
		if t.String() == "interface {}" {
			return "any"
		}
		return t.String()
	}

	// Create a zero value and get its type name
	if t.Kind() == reflect.Ptr {
		// For pointer types, dereference
		return getTypeNameFromType(t.Elem())
	}

	return getTypeName(reflect.Zero(t).Interface())
}

//go:generate go run ../../cmd/tools/gendynamicconfig

package dynamicconfig

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
		key         Key // string value of key. case-insensitive.
		def         T   // default value. cdef is used in preference to def if non-nil.
		cdef        *[]TypedConstrainedValue[T]
		convert     func(any) (T, error) // converter function
		description string               // documentation
	}

	// GenericSetting is an interface that all instances of Setting implement (by generated
	// code in setting_gen.go). It can be used to refer to settings of any type and deal with
	// them generically..
	GenericSetting interface {
		Key() Key
		Precedence() Precedence
		Validate(v any) error

		// for internal use:
		dispatchUpdate(*Collection, any, []ConstrainedValue)
	}
)

package tag

type (
	// Implement Tag interface to supply custom tags to Logger interface implementation.
	Tag interface {
		Key() string
		Value() interface{}
	}
)

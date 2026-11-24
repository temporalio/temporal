package tag

type (
	// Tag is implemented by custom fields supplied to a logger.
	Tag = interface {
		Key() string
		Value() any
	}
)

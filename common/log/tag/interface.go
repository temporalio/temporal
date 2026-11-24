package tag

// Tag is implemented by custom fields supplied to a logger.
type Tag = interface {
	Key() string
	Value() any
}

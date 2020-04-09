package collection

type (
	// Iterator represents the interface for iterator
	Iterator interface {
		// HasNext return whether this iterator has next value
		HasNext() bool
		// Next returns the next item and error
		Next() (interface{}, error)
	}
)

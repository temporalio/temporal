package collection

type (
	// Queue is the interface for queue
	Queue[T any] interface {
		// Peek returns the first item of the queue
		Peek() T
		// Add push an item to the queue
		Add(item T)
		// Remove pop an item from the queue
		Remove() T
		// IsEmpty indicate if the queue is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}

	// HashFunc represents a hash function for string
	HashFunc func(interface{}) uint32

	// ActionFunc take a key and value, do calculation and return err
	ActionFunc func(key interface{}, value interface{}) error
	// PredicateFunc take a key and value, do calculation and return boolean
	PredicateFunc func(key interface{}, value interface{}) bool

	// ConcurrentTxMap is a generic interface for any implementation of a dictionary
	// or a key value lookup table that is thread safe, and providing functionality
	// to modify key / value pair inside within a transaction
	ConcurrentTxMap interface {
		// Get returns the value for the given key
		Get(key interface{}) (interface{}, bool)
		// Contains returns true if the key exist and false otherwise
		Contains(key interface{}) bool
		// Put records the mapping from given key to value
		Put(key interface{}, value interface{})
		// PutIfNotExist records the key value mapping only
		// if the mapping does not already exist
		PutIfNotExist(key interface{}, value interface{}) bool
		// Remove deletes the key from the map
		Remove(key interface{})
		// GetAndDo returns the value corresponding to the key, and apply fn to key value before return value
		// return (value, value exist or not, error when evaluation fn)
		GetAndDo(key interface{}, fn ActionFunc) (interface{}, bool, error)
		// PutOrDo put the key value in the map, if key does not exists, otherwise, call fn with existing key and value
		// return (value, fn evaluated or not, error when evaluation fn)
		PutOrDo(key interface{}, value interface{}, fn ActionFunc) (interface{}, bool, error)
		// RemoveIf deletes the given key from the map if fn return true
		// return whether the key is removed or not
		RemoveIf(key interface{}, fn PredicateFunc) bool
		// Iter returns an iterator to the map
		Iter() MapIterator
		// Len returns the number of items in the map
		Len() int
	}

	// MapIterator represents the interface for map iterators
	MapIterator interface {
		// Close closes the iterator
		// and releases any allocated resources
		Close()
		// Entries returns a channel of MapEntry
		// objects that can be used in a range loop
		Entries() <-chan *MapEntry
	}

	// MapEntry represents a key-value entry within the map
	MapEntry struct {
		// Key represents the key
		Key interface{}
		// Value represents the value
		Value interface{}
	}
)

const (
	// UUIDStringLength is the length of an UUID represented as a hex string
	UUIDStringLength = 36 // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
)

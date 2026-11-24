package testhooks

// keyID is a unique identifier for a key, used as a map key.
type keyID = int64

// Key identifies a hook with value type T and scope type S.
type Key[T any, S any] struct {
	id keyID
}

package umpire

import "fmt"

// Fact is implemented by domain-specific observed facts.
// Each concrete type represents a specific kind of recorded observation.
type Fact interface {
	// Key returns the entity key this fact relates to (e.g. operation ID).
	Key() string
}

// Record wraps a Fact with a sequence number assigned by the Umpire.
type Record struct {
	Seq  int64
	Fact Fact
}

func (r *Record) String() string {
	return fmt.Sprintf("#%d %T key=%s", r.Seq, r.Fact, r.Fact.Key())
}

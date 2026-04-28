package umpire

import (
	"fmt"
	"time"
)

// Fact is implemented by domain-specific observed facts.
// Each concrete type represents a specific kind of recorded observation.
type Fact interface {
	// Key returns the entity key this fact relates to (e.g. operation ID).
	Key() string
}

// Record wraps a Fact with a sequence number assigned by the Umpire.
//
// At is the time the fact was recorded, read from the Umpire's clock (wall
// clock by default; see Umpire.SetClock). Time-bounded rules compare At against
// RuleContext.Now to assert "X happens within Y"; ordering-only rules should
// rely on Seq, which stays deterministic regardless of the clock.
type Record struct {
	Seq  int64
	At   time.Time
	Fact Fact
}

func (r *Record) String() string {
	return fmt.Sprintf("#%d %T key=%s", r.Seq, r.Fact, r.Fact.Key())
}

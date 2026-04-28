package umpire

import "pgregory.net/rapid"

// Input describes a generator for a single action input. Valid produces
// values the system under test should accept; Invalid produces values it
// should reject. Set InvalidProb to a non-zero fraction to occasionally draw
// from Invalid — the second return value of Draw indicates which side fired
// so the caller can switch its expectations:
//
//	opID, invalid := input.Draw(t, "opID")
//	resp, err := client.Start(ctx, opID)
//	if invalid {
//	    require.Error(t, err)
//	} else {
//	    require.NoError(t, err)
//	}
//
// Invalid and InvalidProb are both optional. With Invalid == nil or
// InvalidProb at zero, Draw always returns Valid.
type Input[V any] struct {
	Valid       func(*T, string) V
	Invalid     func(*T, string) V
	InvalidProb Probability
}

// Draw picks a value from Valid or Invalid. The second return is true when
// the Invalid generator was used.
func (i Input[V]) Draw(t *T, label string) (V, bool) {
	t.Helper()
	if i.Invalid != nil && i.InvalidProb.fires(t, label+".invalid?") {
		return i.Invalid(t, label+".invalid"), true
	}
	return i.Valid(t, label), false
}

// Probability is a num/den fraction in [0, 1]. A zero or negative numerator
// disables the event; a numerator >= denominator forces it.
type Probability struct {
	Num int
	Den int
}

func (p Probability) fires(t *T, label string) bool {
	if p.Num <= 0 || p.Den <= 0 {
		return false
	}
	if p.Num >= p.Den {
		return true
	}
	return drawGen(t, label, rapid.IntRange(1, p.Den)) <= p.Num
}

// Pct returns a Probability of n percent. Values <= 0 produce a probability
// that never fires; values >= 100 produce one that always fires.
func Pct(n int) Probability {
	return Probability{Num: n, Den: 100}
}

// OneIn returns a Probability of 1/n (i.e. fires once every n draws on
// average). n <= 1 produces a probability that always fires.
func OneIn(n int) Probability {
	if n <= 1 {
		return Probability{Num: 1, Den: 1}
	}
	return Probability{Num: 1, Den: n}
}

// PickFrom returns a generator that uniformly samples one of items. Empty
// item lists cause Draw to skip the test step.
func PickFrom[V any](items ...V) func(*T, string) V {
	return func(t *T, label string) V {
		t.Helper()
		return Draw(t, label, items)
	}
}

// IntRange returns a generator for ints in [min, max] inclusive.
func IntRange(minValue, maxValue int) func(*T, string) int {
	return func(t *T, label string) int {
		t.Helper()
		return drawGen(t, label, rapid.IntRange(minValue, maxValue))
	}
}

// Bool returns a generator for booleans.
func Bool() func(*T, string) bool {
	return func(t *T, label string) bool {
		t.Helper()
		return drawGen(t, label, rapid.Bool())
	}
}

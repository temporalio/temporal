// GAP (false negative). Effort: L (track signatures for function values, or
// points-to for func-typed variables).
// Ownership flowing through a function VALUE or a closure is invisible: calleeFunc
// resolves only direct func/method calls, so a call through a func-typed variable
// (or a borrowed value captured by a closure) is missed — no signature, so the
// argument is treated as owned.
//
// CURRENT: silent. DESIRED: flag — build embeds its arg, and b is build.
package funcvalue

type Payload struct{}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

func build(m map[string]*Payload) *Resp { return &Resp{Fields: m} }

type builder func(map[string]*Payload) *Resp

type comp struct {
	memo map[string]*Payload
}

func (c *comp) Do() *Resp {
	var b builder = build
	return b(c.memo) // GAP: should be flagged (b == build, which embeds its arg)
}

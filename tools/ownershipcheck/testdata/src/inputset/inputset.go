// INPUT-SET fixture (noise reduction C). A helper that returns one of several
// inputs records the SET of those inputs (viaInput{0,1}) rather than collapsing to
// borrowed. A call site then resolves the set against its actual arguments: owned
// when they are all owned (no false positive), still borrowed when any is borrowed.
package inputset

type Resp struct {
	Fields map[string]string
}

func (*Resp) ProtoReflect() any { return nil }

// pick returns one of its two arguments; inferred result is viaInput{0,1}.
func pick(a, b map[string]string) map[string]string { // want pick:`borrowed result`
	if len(a) > 0 {
		return a
	}
	return b
}

type comp struct {
	shared map[string]string
}

// Merge writes one caller-owned response from another. Both arguments are owned, so
// the picked result is owned and must NOT be flagged — this is the false positive C
// removes (before C, pick's result collapsed to borrowed).
func (c *comp) Merge(into, from *Resp) { // want Merge:`embeds param`
	into.Fields = pick(into.Fields, from.Fields)
}

// Build picks between a borrowed receiver field and an owned parameter; some member
// of the set is borrowed, so the result is borrowed and IS flagged.
func (c *comp) Build(from map[string]string) *Resp { // want Build:`embeds param`
	return &Resp{Fields: pick(c.shared, from)} // want `borrowed map embedded into`
}

// ignoredirective: //ownership:ignore suppresses a finding, but only when it
// carries a reason (so suppressions are reviewable).
package ignoredirective

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type Resp struct {
	Memo *Memo
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	memo map[string]*Payload
}

// Suppressed embeds a borrowed map but is explicitly ignored with a reason.
func (c *comp) Suppressed() *Resp {
	return &Resp{Memo: &Memo{Fields: c.memo}} //ownership:ignore immutable after init, never mutated
}

// NotSuppressed has an ignore directive WITHOUT a reason, so it does not
// suppress and the finding still fires.
func (c *comp) NotSuppressed() *Resp {
	//ownership:ignore
	return &Resp{Memo: &Memo{Fields: c.memo}} // want `borrowed map embedded into`
}

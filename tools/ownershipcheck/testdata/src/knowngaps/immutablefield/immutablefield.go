// GAP FP-2 (false positive). Effort: S (annotate //ownership:result owned) / L
// (auto-prove never-mutated-after-init).
// A receiver field that is set once at construction and never mutated is safe to
// embed, but is flagged as viaReceiver.
//
// CURRENT: flagged. DESIRED: silent. Mitigations today: //ownership:result owned on
// an accessor, or //ownership:ignore at the embed. The remaining gap is auto-proving
// an un-annotated field is never mutated.
package immutablefield

type Payload struct{}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	cfg map[string]*Payload // set once in the constructor, never mutated
}

func (c *comp) Describe() *Resp {
	return &Resp{Fields: c.cfg} // GAP: flagged today, but c.cfg is immutable
}

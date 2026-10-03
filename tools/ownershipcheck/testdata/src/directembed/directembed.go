// DIRECT-EMBED fixture (M1 gate, see plan.md). The live map is read directly
// from a receiver field and embedded into a proto response, so "borrowed" is
// provable without cross-package inference.
package directembed

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type Resp struct {
	Memo *Memo
}

func (*Resp) ProtoReflect() any { return nil }

type svc struct {
	liveMemo map[string]*Payload
}

func (s *svc) Describe() *Resp {
	return &Resp{Memo: &Memo{Fields: s.liveMemo}} // want `borrowed map embedded into`
}

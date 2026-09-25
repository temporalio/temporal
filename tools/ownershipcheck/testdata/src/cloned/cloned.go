// CLONED negative fixture (must stay silent). maps.Clone severs the alias, so
// the embedded value is owned even though it originates from a shared field.
package cloned

import "maps"

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
	return &Resp{Memo: &Memo{Fields: maps.Clone(s.liveMemo)}} // sanitized: clone => owned
}

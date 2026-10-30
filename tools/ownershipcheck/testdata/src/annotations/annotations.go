// annotations: //ownership:result directives override inference in both
// directions — `owned` suppresses a would-be finding, `borrowed` forces one.
package annotations

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

// SafeMemo returns a receiver field by reference (inference would call it
// borrowed), but it is annotated owned (e.g. the map is immutable after init).
//
//ownership:result owned
func (c *comp) SafeMemo() map[string]*Payload {
	return c.memo
}

// ExternalMemo returns a freshly made map (inference would call it owned), but is
// annotated borrowed to force flagging — e.g. the real source is reached through
// an interface that inference cannot see.
//
//ownership:result borrowed
func ExternalMemo() map[string]*Payload { // want ExternalMemo:`borrowed result`
	return make(map[string]*Payload)
}

func (c *comp) DescribeSafe() *Resp {
	return &Resp{Memo: &Memo{Fields: c.SafeMemo()}} // annotated owned -> silent
}

func DescribeExternal() *Resp {
	return &Resp{Memo: &Memo{Fields: ExternalMemo()}} // want `borrowed map embedded into`
}

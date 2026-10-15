// FRESH-SEND negative fixture (must stay silent). The map is produced locally
// and never published to shared state, so it is the sole owner and safe to send.
package freshsend

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type Resp struct {
	Memo *Memo
}

func (*Resp) ProtoReflect() any { return nil }

func Describe() *Resp {
	m := make(map[string]*Payload)
	m["k"] = &Payload{}
	return &Resp{Memo: &Memo{Fields: m}} // owned: freshly made, never published
}

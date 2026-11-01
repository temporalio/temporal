// namedreturn: a borrowed value written into a named result proto (which is then
// naked-returned) escapes and is flagged — named results always escape.
package namedreturn

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

func (c *comp) Describe() (resp *Resp) {
	resp = &Resp{Memo: &Memo{}}
	resp.Memo.Fields = c.memo // want `borrowed map assigned to`
	return
}

func (c *comp) DescribeOwned() (resp *Resp) {
	resp = &Resp{Memo: &Memo{}}
	resp.Memo.Fields = make(map[string]*Payload) // owned -> silent
	return
}

// fieldassign: the field-assignment sink — `resp.Field = borrowed` on a proto
// that escapes (is returned) is flagged, just like a composite-literal embed. An
// owned RHS, or a proto that does not escape, stays silent.
package fieldassign

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

func (c *comp) Describe() *Resp {
	resp := &Resp{Memo: &Memo{}}
	resp.Memo.Fields = c.memo // want `borrowed map assigned to`
	return resp
}

func (c *comp) DescribeOwned() *Resp {
	resp := &Resp{Memo: &Memo{}}
	resp.Memo.Fields = make(map[string]*Payload) // owned -> silent
	return resp
}

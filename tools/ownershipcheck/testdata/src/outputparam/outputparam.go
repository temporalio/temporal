// outputparam: a borrowed map/slice written into a proto-pointer parameter (an
// output proto the caller owns and may marshal) is flagged, just like a returned
// proto. An owned RHS stays silent.
package outputparam

type Payload struct{}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	memo map[string]*Payload
}

func (c *comp) Fill(out *Resp) {
	out.Fields = c.memo // want `borrowed map assigned to`
}

func (c *comp) FillOwned(out *Resp) {
	out.Fields = make(map[string]*Payload) // owned -> silent
}

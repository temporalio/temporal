// BYTE-SLICE fixture (noise reduction B). A borrowed []byte is a blob/token, not a
// mutable collection iterated during marshal, so it is not flagged; a borrowed
// non-byte slice still is. Runs under the default -value-kinds=map,slice.
package byteslice

type Resp struct {
	Token []byte
	Names []string
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	token []byte   // immutable serialized token
	names []string // a mutable collection
}

func (c *comp) Describe() *Resp {
	return &Resp{
		Token: c.token, // []byte: not a hazard leaf -> silent
		Names: c.names, // []string: flagged // want `borrowed slice embedded into`
	}
}

// flowresult: signature inference is flow-sensitive too. A method that clones its
// receiver field on only some paths returns borrowed data on the others, so its
// result is inferred borrowed — and a caller embedding that result is flagged.
// (Flow-insensitive inference, last-write-wins, would have called it owned → FN.)
package flowresult

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

type comp struct {
	memo map[string]*Payload
}

// maybeClone clones on one path only: the fall-through returns the live field.
func (c *comp) maybeClone(rare bool) map[string]*Payload { // want maybeClone:`borrowed result`
	m := c.memo
	if !rare {
		m = maps.Clone(c.memo)
	}
	return m
}

func (c *comp) Describe(rare bool) *Resp {
	return &Resp{Memo: &Memo{Fields: c.maybeClone(rare)}} // want `borrowed map embedded into`
}

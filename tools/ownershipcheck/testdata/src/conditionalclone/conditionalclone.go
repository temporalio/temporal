// conditionalclone: flow-sensitive ownership. A value cloned on only some paths is
// borrowed at a merge point (borrowed wins), so embedding it is flagged; a value
// cloned on every path is owned and stays silent.
package conditionalclone

import "maps"

type Payload struct{}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	a map[string]*Payload
	b map[string]*Payload
}

// Conditional clones on one path only: the fall-through path leaves m borrowed.
func (c *comp) Describe(rare bool) *Resp {
	m := c.a
	if !rare {
		m = maps.Clone(c.a)
	}
	return &Resp{Fields: m} // want `borrowed map embedded into`
}

// Cloned on every path: owned at the merge, silent.
func (c *comp) DescribeSafe(rare bool) *Resp {
	var m map[string]*Payload
	if rare {
		m = maps.Clone(c.a)
	} else {
		m = maps.Clone(c.b)
	}
	return &Resp{Fields: m} // owned on both paths -> silent
}

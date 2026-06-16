// subslice (FN-3): a re-slice of a borrowed slice shares its backing array, so it
// is itself borrowed and must be flagged when embedded into an escaping proto.
package subslice

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	items []*Item
}

func (c *comp) Describe() *Resp {
	return &Resp{Items: c.items[:1]} // want `borrowed slice embedded into`
}

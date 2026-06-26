// unknownslicemutation: passing a borrowed slice to an opaque callee keeps the
// source conservative, because the callee may retain or mutate it.
package unknownslicemutation

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	items []*Item
}

func mutate([]*Item) {}

func (c *comp) MaybeMutate() {
	mutate(c.items)
}

func (c *comp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

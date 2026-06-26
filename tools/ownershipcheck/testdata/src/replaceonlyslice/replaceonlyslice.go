// replaceonlyslice: a borrowed slice field is silent when all visible writes
// replace the field with a fresh backing array rather than mutating it in place.
package replaceonlyslice

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	items []*Item
}

func (c *comp) Refresh() {
	items := make([]*Item, 0, 1)
	items = append(items, &Item{})
	c.items = items
}

func (c *comp) Reset() {
	c.items = []*Item{}
}

func (c *comp) Clear() {
	c.items = nil
}

func (c *comp) Describe() *Resp {
	return &Resp{Items: c.items}
}

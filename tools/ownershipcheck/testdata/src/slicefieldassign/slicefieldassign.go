// slicefieldassign: field-assignment sinks use the same slice mutability gate as
// composite literal embeds.
package slicefieldassign

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type replaceOnlyComp struct {
	items []*Item
}

func (c *replaceOnlyComp) Refresh() {
	items := make([]*Item, 0, 1)
	items = append(items, &Item{})
	c.items = items
}

func (c *replaceOnlyComp) Describe() *Resp {
	resp := &Resp{}
	resp.Items = c.items
	return resp
}

type mutableComp struct {
	items []*Item
}

func (c *mutableComp) Add(item *Item) {
	c.items = append(c.items, item)
}

func (c *mutableComp) Describe() *Resp {
	resp := &Resp{}
	resp.Items = c.items // want `borrowed slice assigned to`
	return resp
}

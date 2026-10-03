// slicealiasmutation: local aliases and subslices preserve the field source, so
// mutations through those aliases keep the eventual embed reportable.
package slicealiasmutation

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type aliasComp struct {
	items []*Item
}

func (c *aliasComp) Set(item *Item) {
	items := c.items
	items[0] = item
}

func (c *aliasComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type subsliceComp struct {
	items []*Item
}

func (c *subsliceComp) Set(item *Item) {
	items := c.items[:1]
	items[0] = item
}

func (c *subsliceComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

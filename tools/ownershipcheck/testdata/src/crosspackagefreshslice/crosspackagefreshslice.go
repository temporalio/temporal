// crosspackagefreshslice: imported fresh-result facts distinguish a helper that
// allocates a new backing array from one that returns its input.
package crosspackagefreshslice

import "freshslicelib"

type Resp struct {
	Items []*freshslicelib.Item
}

func (*Resp) ProtoReflect() any { return nil }

type freshComp struct {
	items []*freshslicelib.Item
}

func (c *freshComp) Refresh() {
	c.items = freshslicelib.Build()
}

func (c *freshComp) Describe() *Resp {
	return &Resp{Items: c.items}
}

type borrowedComp struct {
	items []*freshslicelib.Item
}

func (c *borrowedComp) Refresh() {
	c.items = freshslicelib.PassThrough(c.items)
}

func (c *borrowedComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type aliasedComp struct {
	items []*freshslicelib.Item
}

func (c *aliasedComp) Refresh() {
	items, alias := freshslicelib.BuildPair()
	c.items = items
	alias[0] = &freshslicelib.Item{}
}

func (c *aliasedComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type independentComp struct {
	items []*freshslicelib.Item
}

func (c *independentComp) Refresh() {
	items, other := freshslicelib.BuildIndependentPair()
	c.items = items
	other[0] = &freshslicelib.Item{}
}

func (c *independentComp) Describe() *Resp {
	return &Resp{Items: c.items}
}

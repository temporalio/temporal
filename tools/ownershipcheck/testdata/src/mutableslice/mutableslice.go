// mutableslice: borrowed slice fields still report when any visible write may
// mutate the backing array in place.
package mutableslice

import "sort"

type Item struct {
	ID int
}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type appendComp struct {
	items []*Item
}

func (c *appendComp) Add(item *Item) {
	c.items = append(c.items, item)
}

func (c *appendComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type indexComp struct {
	items []*Item
}

func (c *indexComp) Set(item *Item) {
	c.items[0] = item
}

func (c *indexComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type copyComp struct {
	items []*Item
}

func (c *copyComp) Copy(src []*Item) {
	copy(c.items, src)
}

func (c *copyComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type sortComp struct {
	items []*Item
}

func (c *sortComp) Sort() {
	sort.Slice(c.items, func(i, j int) bool { return c.items[i].ID < c.items[j].ID })
}

func (c *sortComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

// freshslicehelper: helper results are replace-only only when every return path
// yields a fresh slice and the helper does not retain an alias.
package freshslicehelper

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

func buildItems() ([]*Item, error) { // want buildItems:`fresh result`
	items := make([]*Item, 0, 1)
	items = append(items, &Item{})
	return items, nil
}

type freshComp struct {
	items []*Item
}

func (c *freshComp) Refresh() {
	items, err := buildItems()
	if err != nil {
		return
	}
	c.items = items
}

func (c *freshComp) Describe() *Resp {
	return &Resp{Items: c.items}
}

func buildItemsViaVar() []*Item { // want buildItemsViaVar:`fresh result`
	var items, err = buildItems()
	if err != nil {
		return nil
	}
	return items
}

type varComp struct {
	items []*Item
}

func (c *varComp) Refresh() {
	c.items = buildItemsViaVar()
}

func (c *varComp) Describe() *Resp {
	return &Resp{Items: c.items}
}

func passThrough(items []*Item) ([]*Item, error) { // want passThrough:`borrowed result`
	return items, nil
}

type borrowedComp struct {
	items []*Item
}

func (c *borrowedComp) Refresh() {
	items, _ := passThrough(c.items)
	c.items = items
}

func (c *borrowedComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

var retained []*Item

func buildAndRetain() []*Item {
	items := []*Item{{}}
	retained = items
	return items
}

type retainedComp struct {
	items []*Item
}

func (c *retainedComp) Refresh() {
	c.items = buildAndRetain()
}

func (c *retainedComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

type holder struct {
	items []*Item
}

var retainedHolder *holder

func buildAndRetainAggregate() []*Item {
	items := []*Item{{}}
	retainedHolder = &holder{items: items}
	return items
}

type aggregateComp struct {
	items []*Item
}

func (c *aggregateComp) Refresh() {
	c.items = buildAndRetainAggregate()
}

func (c *aggregateComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

var mutateLater func()

func buildAndCapture() []*Item {
	items := []*Item{{}}
	mutateLater = func() {
		items[0] = &Item{}
	}
	return items
}

type capturedComp struct {
	items []*Item
}

func (c *capturedComp) Refresh() {
	c.items = buildAndCapture()
}

func (c *capturedComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

func buildAndSend(ch chan<- []*Item) []*Item {
	items := []*Item{{}}
	ch <- items
	return items
}

type sentComp struct {
	items []*Item
}

func (c *sentComp) Refresh(ch chan<- []*Item) {
	c.items = buildAndSend(ch)
}

func (c *sentComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

func buildAndSelectSend(ch chan<- []*Item) []*Item {
	items := []*Item{{}}
	select {
	case ch <- items:
	default:
	}
	return items
}

type selectSentComp struct {
	items []*Item
}

func (c *selectSentComp) Refresh(ch chan<- []*Item) {
	c.items = buildAndSelectSend(ch)
}

func (c *selectSentComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

func buildWithShadowedAppend() []*Item {
	items := []*Item{{}}
	append := func(items []*Item) []*Item {
		retained = items
		return items
	}
	return append(items)
}

type shadowedBuiltinComp struct {
	items []*Item
}

func (c *shadowedBuiltinComp) Refresh() {
	c.items = buildWithShadowedAppend()
}

func (c *shadowedBuiltinComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

func buildAliasedPair() ([]*Item, []*Item) {
	items := []*Item{{}}
	return items, items
}

type aliasedResultsComp struct {
	items []*Item
}

func (c *aliasedResultsComp) Refresh() {
	items, alias := buildAliasedPair()
	c.items = items
	alias[0] = &Item{}
}

func (c *aliasedResultsComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

func buildConditional(items []*Item, useFresh bool) []*Item { // want buildConditional:`borrowed result`
	if useFresh {
		return []*Item{}
	}
	return items
}

type conditionalComp struct {
	items []*Item
}

func (c *conditionalComp) Refresh(useFresh bool) {
	c.items = buildConditional(c.items, useFresh)
}

func (c *conditionalComp) Describe() *Resp {
	return &Resp{Items: c.items} // want `borrowed slice embedded into`
}

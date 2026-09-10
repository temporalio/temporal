// GAP FP-1 (false positive). Effort: M–L (distinguish leased/component receivers
// from plain local structs; allowlist or reachability).
// The "receiver == shared" assumption is false for a struct constructed locally
// and used as a one-shot builder. Its fields are effectively owned by the caller.
//
// CURRENT: flagged. DESIRED: silent (builder is a fresh local, never shared).
package nonsharedrecv

type Item struct{}

type Resp struct {
	Items []*Item
}

func (*Resp) ProtoReflect() any { return nil }

type builder struct {
	items []*Item
}

func (b *builder) build() *Resp {
	return &Resp{Items: b.items} // GAP: flagged today, but b is a fresh local builder
}

func New(items []*Item) *Resp {
	b := &builder{items: items}
	return b.build()
}

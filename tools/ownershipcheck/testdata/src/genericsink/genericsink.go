// genericsink proves the checker is domain-agnostic: with -sink=Sealed, a type
// marked by a Sealed method (not a proto) is the sink, and embedding a borrowed
// slice into it is flagged. No proto knowledge in the core.
package genericsink

type Payload struct{}

// Envelope is the sink under this config (marker method Sealed); it is not a proto.
type Envelope struct {
	Items []*Payload
}

func (*Envelope) Sealed() {}

type comp struct {
	items []*Payload
}

func (c *comp) Build() *Envelope {
	return &Envelope{Items: c.items} // want `borrowed slice embedded into`
}

func (c *comp) BuildFresh() *Envelope {
	return &Envelope{Items: []*Payload{}} // owned -> silent
}

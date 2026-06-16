// ifacesink proves a qualified -sink=ifacesink.Sealer matches by interface
// implementation (types.Implements), not just by method name: Envelope implements
// Sealer and is a sink, while Faux has a same-named method with the wrong signature
// and is therefore NOT a sink (a bare method-name match would have flagged it).
package ifacesink

type Payload struct{}

type Sealer interface{ Seal() }

type Envelope struct {
	Items []*Payload
}

func (*Envelope) Seal() {} // implements Sealer -> a sink

type Faux struct {
	Items []*Payload
}

func (*Faux) Seal(int) {} // wrong signature -> does NOT implement Sealer

type comp struct {
	items []*Payload
}

func (c *comp) Build() *Envelope {
	return &Envelope{Items: c.items} // want `borrowed slice embedded into`
}

func (c *comp) BuildFaux() *Faux {
	return &Faux{Items: c.items} // Faux is not a Sealer -> not a sink -> silent
}

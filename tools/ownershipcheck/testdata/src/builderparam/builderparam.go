// builderparam: a borrowed value passed to a helper that embeds its parameter
// into an escaping proto is flagged at the call site (the borrowed value leaks
// through the builder). The builder's parameter is summarized as "embeds param".
package builderparam

type Payload struct{}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

// build embeds its parameter into a returned proto (summarized: embeds param 0).
func build(m map[string]*Payload) *Resp { // want build:`embeds param`
	return &Resp{Fields: m}
}

type comp struct {
	memo map[string]*Payload
}

func (c *comp) Do() *Resp {
	return build(c.memo) // want `borrowed map passed to build, which embeds it`
}

func (c *comp) DoOwned() *Resp {
	return build(make(map[string]*Payload)) // owned arg -> silent
}

// builder is NOT a proto message.
type builder struct {
	items map[string]*Payload
}

// newBuilder embeds its parameter into a non-proto struct — that is not the
// marshal hazard, so its Embedded summary must be empty. Regression for the
// historybuilder.New false positive (embedding into a non-proto builder).
func newBuilder(m map[string]*Payload) *builder {
	return &builder{items: m}
}

func (c *comp) UseBuilder() *builder {
	return newBuilder(c.memo) // non-proto target -> must stay silent
}

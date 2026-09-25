// paramescapes: //ownership:param <name> escapes declares that a parameter's data
// is retained or marshaled by the callee (e.g. an RPC sender, opaque to inference).
// Passing a borrowed value to such a parameter is then flagged at the call site.
package paramescapes

type Payload struct{}

type Req struct {
	Fields map[string]*Payload
}

func (*Req) ProtoReflect() any { return nil }

type comp struct {
	memo map[string]*Payload
}

// store retains its map argument.
//
//ownership:param m escapes
func store(m map[string]*Payload) {} // want store:`embeds param`

// send marshals its request proto.
//
//ownership:param r escapes
func send(r *Req) {} // want send:`embeds param`

func (c *comp) Do() {
	store(c.memo) // want `borrowed map passed to store`
}

func (c *comp) Send() {
	send(&Req{Fields: c.memo}) // want `borrowed map embedded into`
}

func (c *comp) Safe() {
	store(make(map[string]*Payload)) // owned arg -> silent
}

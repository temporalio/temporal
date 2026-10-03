// namedresult: //ownership:result <name> targets one result of a multi-return
// function (the bare form applies to all). Here only the `shared` result is
// declared borrowed; the `fresh` result stays owned.
package namedresult

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type Resp struct {
	Memo *Memo
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	memo map[string]*Payload
}

// split returns a fresh copy and the live (shared) map; only `shared` is borrowed.
//
//ownership:result shared borrowed
func (c *comp) split() (fresh, shared map[string]*Payload) { // want split:`borrowed result`
	return make(map[string]*Payload), c.memo
}

func (c *comp) UseShared() *Resp {
	_, shared := c.split()
	return &Resp{Memo: &Memo{Fields: shared}} // want `borrowed map embedded into`
}

func (c *comp) UseFresh() *Resp {
	fresh, _ := c.split()
	return &Resp{Memo: &Memo{Fields: fresh}} // owned result -> silent
}

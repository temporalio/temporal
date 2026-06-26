// escapelocal: a temp proto is built and consumed synchronously in-function (read
// for a local computation, never returned). The borrowed alias never reaches an
// out-of-lock marshal, so it must stay silent (mirrors pRef.Marshal() and the
// replication VersionHistory read-only cases).
package escapelocal

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

func (m *Memo) size() int { return len(m.Fields) }

type comp struct {
	memo map[string]*Payload
}

func (c *comp) Size() int {
	tmp := &Memo{Fields: c.memo} // consumed locally, never returned -> silent
	return tmp.size()
}

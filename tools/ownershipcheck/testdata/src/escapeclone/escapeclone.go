// escapeclone: the build-then-clone idiom must stay silent. A temp proto aliases
// the live map, but only its deep clone is returned — the alias never escapes, so
// the synchronous read during cloning is safe (mirrors MutableStateImpl.CloneToProto).
package escapeclone

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type comp struct {
	memo map[string]*Payload
}

func (c *comp) CloneToProto() *Memo {
	tmp := &Memo{Fields: c.memo} // not returned; only its clone is -> silent
	return cloneMemo(tmp)
}

func cloneMemo(m *Memo) *Memo {
	out := &Memo{Fields: make(map[string]*Payload, len(m.Fields))}
	for k, v := range m.Fields {
		out.Fields[k] = v
	}
	return out
}

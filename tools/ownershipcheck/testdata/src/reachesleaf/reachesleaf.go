// REACHES-LEAF fixture (noise reduction A). With pointer tracking on, a borrowed
// pointer is flagged only if its pointee transitively contains a map/slice leaf —
// the kinds that actually crash on concurrent access. A pointer to a scalar-only
// message (a timestamp/duration shape) can never produce the crash, so it is silent
// even though it is borrowed and escapes. Run with -value-kinds=map,slice,pointer.
package reachesleaf

// Scalar has only scalar fields: no hazard leaf is reachable through *Scalar.
type Scalar struct {
	Seconds int64
	Nanos   int32
}

func (*Scalar) ProtoReflect() any { return nil }

// WithMap transitively contains a map: a borrowed *WithMap is a real hazard.
type WithMap struct {
	Fields map[string]string
}

func (*WithMap) ProtoReflect() any { return nil }

type Resp struct {
	S *Scalar
	W *WithMap
}

func (*Resp) ProtoReflect() any { return nil }

type comp struct {
	s *Scalar
	w *WithMap
}

func (c *comp) Describe() *Resp {
	return &Resp{
		S: c.s, // scalar-only pointee: borrowed but never a concurrent-mutation hazard -> silent
		W: c.w, // pointee contains a map: flagged // want `borrowed pointer embedded into`
	}
}

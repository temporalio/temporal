// GAP FN-5 (false negative). Effort: S (annotate the interface method) / L (auto
// via implementation analysis).
// When the borrowed source is reached through an interface method, inference has
// no body to analyze, so no fact is produced and the result is treated as owned.
//
// CURRENT: silent. DESIRED: flag (or require //ownership:result borrowed on the
// interface method).
package ifacemethod

type Payload struct{}

type Visibility interface {
	Memo() map[string]*Payload
}

type Resp struct {
	Fields map[string]*Payload
}

func (*Resp) ProtoReflect() any { return nil }

func Describe(v Visibility) *Resp {
	return &Resp{Fields: v.Memo()} // GAP: should be flagged (interface method returns a live map)
}

// genericfield regression fixture: mirrors chasm.Field[T].Get/TryGet. Exercises
// the full inference chain that the scheduler's FutureActionTimes leak needs —
// generics (Origin-keyed facts), comma-ok type assertion, comma-ok call, and
// multi-level delegation — all rooted at the receiver and resolved with zero
// annotations.
package genericfield

type Payload struct{}

// Field mirrors chasm.Field[T]: Get delegates to TryGet, which returns a value
// read out of the receiver via a comma-ok type assertion.
type Field[T any] struct {
	v any
}

func (f Field[T]) TryGet() (T, bool) { // want TryGet:`borrowed result`
	vT, ok := f.v.(T) // comma-ok type assertion of a receiver field
	return vT, ok
}

func (f Field[T]) Get() T { // want Get:`borrowed result`
	v, _ := f.TryGet()
	return v
}

type Generator struct {
	FutureActionTimes []int64
}

type Resp struct {
	Times []int64
}

func (*Resp) ProtoReflect() any { return nil }

type Scheduler struct {
	Generator Field[*Generator]
}

func (s *Scheduler) ListInfo() *Resp {
	generator := s.Generator.Get()
	return &Resp{Times: generator.FutureActionTimes} // want `borrowed slice embedded into`
}

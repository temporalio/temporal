// Package protofield provides typed, exhaustively-enumerable handles to the
// fields of a protobuf message.
//
// For each message, generated code (see cmd/tools/genprotofields) emits one
// handle per field. A handle knows the field's proto and Go names, its kind,
// how to read and write the field, and whether it is set. Downstream tools
// build default requests, match responses, and validate requests by walking
// these handles instead of reflecting at the call site.
//
// The handle set for a message is the single source of exhaustiveness: because
// each handle is enumerated, a consumer can be forced to account for every
// field, and Verify fails a test when a proto field is added without
// regenerating the handles.
package protofield

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

// Kind classifies a proto field by its cardinality and value type.
type Kind int

const (
	KindScalar   Kind = iota // string, bool, bytes, or numeric
	KindEnum                 // enum (represented as a named integer in Go)
	KindMessage              // singular message
	KindRepeated             // repeated scalar, enum, or message
	KindMap                  // map field
	KindOneof                // oneof group (a single Go interface field)
)

// Field is the type-erased view of a single message field. It is the common
// interface over the typed handles below, enabling exhaustive iteration over
// every field of a message M.
type Field[M proto.Message] interface {
	// ProtoName is the field's proto name, e.g. "workflow_id". For a oneof
	// handle it is the oneof's name.
	ProtoName() string
	// GoName is the field's Go struct name, e.g. "WorkflowId".
	GoName() string
	// Kind classifies the field.
	Kind() Kind
	// IsSet reports whether the field holds a non-zero value on m.
	IsSet(m M) bool
}

// meta carries the shared metadata and the generated is-set closure embedded by
// every typed handle.
type meta[M proto.Message] struct {
	protoName string
	goName    string
	kind      Kind
	isSet     func(M) bool
}

func (m meta[M]) ProtoName() string { return m.protoName }
func (m meta[M]) GoName() string    { return m.goName }
func (m meta[M]) Kind() Kind        { return m.kind }
func (m meta[M]) IsSet(msg M) bool  { return m.isSet(msg) }

// Scalar is a handle to a singular scalar or enum field of type V on message M.
type Scalar[M proto.Message, V any] struct {
	meta[M]
	get func(M) V
	set func(M, V)
}

// Get returns the field's value on m.
func (f Scalar[M, V]) Get(m M) V { return f.get(m) }

// Set assigns v to the field on m.
func (f Scalar[M, V]) Set(m M, v V) { f.set(m, v) }

// NewScalar builds a scalar or enum field handle. kind must be KindScalar or
// KindEnum.
func NewScalar[M proto.Message, V any](
	protoName, goName string, kind Kind,
	isSet func(M) bool, get func(M) V, set func(M, V),
) Scalar[M, V] {
	return Scalar[M, V]{meta[M]{protoName, goName, kind, isSet}, get, set}
}

// Message is a handle to a singular message field of type V on message M.
type Message[M proto.Message, V proto.Message] struct {
	meta[M]
	get func(M) V
	set func(M, V)
}

// Get returns the field's value on m (nil if unset).
func (f Message[M, V]) Get(m M) V { return f.get(m) }

// Set assigns v to the field on m.
func (f Message[M, V]) Set(m M, v V) { f.set(m, v) }

// NewMessage builds a singular message field handle.
func NewMessage[M proto.Message, V proto.Message](
	protoName, goName string,
	isSet func(M) bool, get func(M) V, set func(M, V),
) Message[M, V] {
	return Message[M, V]{meta[M]{protoName, goName, KindMessage, isSet}, get, set}
}

// Repeated is a handle to a repeated field with element type E on message M.
type Repeated[M proto.Message, E any] struct {
	meta[M]
	get func(M) []E
	set func(M, []E)
}

// Get returns the field's slice on m (nil if unset).
func (f Repeated[M, E]) Get(m M) []E { return f.get(m) }

// Set assigns v to the field on m.
func (f Repeated[M, E]) Set(m M, v []E) { f.set(m, v) }

// NewRepeated builds a repeated field handle.
func NewRepeated[M proto.Message, E any](
	protoName, goName string,
	isSet func(M) bool, get func(M) []E, set func(M, []E),
) Repeated[M, E] {
	return Repeated[M, E]{meta[M]{protoName, goName, KindRepeated, isSet}, get, set}
}

// Map is a handle to a map field with key type K and value type V on message M.
type Map[M proto.Message, K comparable, V any] struct {
	meta[M]
	get func(M) map[K]V
	set func(M, map[K]V)
}

// Get returns the field's map on m (nil if unset).
func (f Map[M, K, V]) Get(m M) map[K]V { return f.get(m) }

// Set assigns v to the field on m.
func (f Map[M, K, V]) Set(m M, v map[K]V) { f.set(m, v) }

// NewMap builds a map field handle.
func NewMap[M proto.Message, K comparable, V any](
	protoName, goName string,
	isSet func(M) bool, get func(M) map[K]V, set func(M, map[K]V),
) Map[M, K, V] {
	return Map[M, K, V]{meta[M]{protoName, goName, KindMap, isSet}, get, set}
}

// Oneof is a handle to a oneof group on message M. V is the Go wrapper value
// for the set member (an interface type whose concrete value selects the
// member). It is read-only: the wrapper interface is unexported in the message's
// own package, so it cannot be assigned from generated code in another package.
// Set the member directly on the message literal instead.
type Oneof[M proto.Message, V any] struct {
	meta[M]
	get func(M) V
}

// Get returns the oneof's wrapper value on m (nil if no member is set).
func (f Oneof[M, V]) Get(m M) V { return f.get(m) }

// NewOneof builds a oneof group handle. protoName is the oneof's name.
func NewOneof[M proto.Message, V any](
	protoName, goName string,
	isSet func(M) bool, get func(M) V,
) Oneof[M, V] {
	return Oneof[M, V]{meta[M]{protoName, goName, KindOneof, isSet}, get}
}

// Verify checks that fields enumerates exactly the fields declared on m's
// descriptor: every non-oneof proto field is covered by a non-oneof handle, and
// every (non-synthetic) oneof group is covered by a oneof handle. It returns an
// error describing any proto fields or oneofs missing from the handle set, or
// any handles that no longer exist on the message.
//
// Proto3 optional fields, which the runtime models as synthetic oneofs, are
// treated as ordinary scalar fields (matched by their member name).
//
// Generated tests call Verify so that adding a proto field without
// regenerating the handles fails the build.
func Verify[M proto.Message](m M, fields []Field[M]) error {
	desc := m.ProtoReflect().Descriptor()

	wantFields := map[string]bool{}
	fds := desc.Fields()
	for i := 0; i < fds.Len(); i++ {
		f := fds.Get(i)
		if oo := f.ContainingOneof(); oo != nil && !oo.IsSynthetic() {
			continue // covered by the oneof group, not individually
		}
		wantFields[string(f.Name())] = true
	}

	wantOneofs := map[string]bool{}
	oos := desc.Oneofs()
	for i := 0; i < oos.Len(); i++ {
		oo := oos.Get(i)
		if oo.IsSynthetic() {
			continue // proto3 optional; its member is matched as a field
		}
		wantOneofs[string(oo.Name())] = true
	}

	var extra []string
	for _, f := range fields {
		name := f.ProtoName()
		if f.Kind() == KindOneof {
			if !wantOneofs[name] {
				extra = append(extra, "oneof "+name)
				continue
			}
			delete(wantOneofs, name)
			continue
		}
		if !wantFields[name] {
			extra = append(extra, name)
			continue
		}
		delete(wantFields, name)
	}

	var missing []string
	for name := range wantFields {
		missing = append(missing, name)
	}
	for name := range wantOneofs {
		missing = append(missing, "oneof "+name)
	}

	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(extra)
	var b strings.Builder
	fmt.Fprintf(&b, "field handles for %s are out of sync with the proto", desc.FullName())
	if len(missing) > 0 {
		fmt.Fprintf(&b, "; missing handles for: %s", strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		fmt.Fprintf(&b, "; handles for unknown fields: %s", strings.Join(extra, ", "))
	}
	b.WriteString(" (regenerate with `make proto`)")
	return fmt.Errorf("%s", b.String())
}

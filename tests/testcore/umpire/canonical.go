package umpire

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// idRewriter maps nondeterministic raw values (run IDs, request IDs, tokens,
// timestamps) to stable first-seen indices, so two runs that differ only in
// such values canonicalize identically. Indices are assigned in first-seen
// order, which is itself deterministic given a fixed rapid seed (observation
// order is fixed by Record.Seq). Not safe for concurrent use — one rewriter
// belongs to one trace.
type idRewriter struct {
	seen map[string]int
}

func newIDRewriter() *idRewriter { return &idRewriter{seen: make(map[string]int)} }

func (r *idRewriter) rewrite(raw string) string {
	if r == nil {
		return raw
	}
	idx, ok := r.seen[raw]
	if !ok {
		idx = len(r.seen)
		r.seen[raw] = idx
	}
	return "#" + strconv.Itoa(idx)
}

// nondeterministicFieldNames are proto field names whose values are
// server-assigned or otherwise vary run-to-run and so must be normalized
// before hashing. The list is intentionally incomplete on day one — extend it
// as real divergences are triaged (see plan.L0-divergence.md).
var nondeterministicFieldNames = map[string]bool{
	"run_id":          true,
	"request_id":      true,
	"task_token":      true,
	"long_poll_token": true,
	"clock":           true,
}

// isNondeterministicField reports whether a field's value should be replaced
// with a first-seen index rather than hashed verbatim. Timestamps (*_time,
// *_timestamp) are normalized; durations (*_timeout) are not — those are drawn
// deterministically by rapid and are part of the tested behavior.
func isNondeterministicField(fd protoreflect.FieldDescriptor) bool {
	name := string(fd.Name())
	if nondeterministicFieldNames[name] {
		return true
	}
	return strings.HasSuffix(name, "_time") || strings.HasSuffix(name, "_timestamp")
}

// canonicalProtoBytes produces a stable textual encoding of m suitable for
// hashing. Map keys are sorted, fields are emitted in field-number order, and
// nondeterministic fields are rewritten via rw. proto.Marshal is deliberately
// avoided — wire encoding is not stable across proto versions or map ordering.
func canonicalProtoBytes(m proto.Message, rw *idRewriter) []byte {
	if m == nil {
		return []byte("<nil>")
	}
	var b strings.Builder
	appendMessage(&b, m.ProtoReflect(), rw)
	return []byte(b.String())
}

func appendMessage(b *strings.Builder, m protoreflect.Message, rw *idRewriter) {
	if m == nil || !m.IsValid() {
		b.WriteString("<nil>")
		return
	}
	b.WriteByte('{')
	b.WriteString(string(m.Descriptor().FullName()))

	type setField struct {
		fd protoreflect.FieldDescriptor
		v  protoreflect.Value
	}
	var fields []setField
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		fields = append(fields, setField{fd, v})
		return true
	})
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].fd.Number() < fields[j].fd.Number()
	})

	for _, f := range fields {
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(int(f.fd.Number())))
		b.WriteByte(':')
		appendField(b, f.fd, f.v, rw)
	}
	b.WriteByte('}')
}

func appendField(b *strings.Builder, fd protoreflect.FieldDescriptor, v protoreflect.Value, rw *idRewriter) {
	switch {
	case fd.IsList():
		list := v.List()
		b.WriteByte('[')
		for i := 0; i < list.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			appendValue(b, fd, list.Get(i), rw)
		}
		b.WriteByte(']')
	case fd.IsMap():
		type entry struct {
			k protoreflect.MapKey
			v protoreflect.Value
		}
		var entries []entry
		v.Map().Range(func(k protoreflect.MapKey, val protoreflect.Value) bool {
			entries = append(entries, entry{k, val})
			return true
		})
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].k.String() < entries[j].k.String()
		})
		b.WriteByte('<')
		for i, e := range entries {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(e.k.String())
			b.WriteByte('=')
			appendValue(b, fd.MapValue(), e.v, rw)
		}
		b.WriteByte('>')
	default:
		appendValue(b, fd, v, rw)
	}
}

func appendValue(b *strings.Builder, fd protoreflect.FieldDescriptor, v protoreflect.Value, rw *idRewriter) {
	// A nondeterministic field — scalar or whole message subtree — is replaced
	// by a first-seen index. The raw content (rw == nil) keys the rewriter so
	// the same logical value maps to the same index within a trace.
	if rw != nil && isNondeterministicField(fd) {
		var raw strings.Builder
		appendValue(&raw, fd, v, nil)
		b.WriteString(rw.rewrite(raw.String()))
		return
	}
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		appendMessage(b, v.Message(), rw)
		return
	}
	if fd.Kind() == protoreflect.BytesKind {
		b.WriteString(hex.EncodeToString(v.Bytes()))
		return
	}
	b.WriteString(v.String())
}

var (
	uuidPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	timePattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})`)
)

// canonicalErr reduces an error to its gRPC code plus a message with UUIDs and
// RFC3339 timestamps stripped, so incidental identifiers in error text don't
// cause false divergence.
func canonicalErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	msg := err.Error()
	msg = uuidPattern.ReplaceAllString(msg, "<uuid>")
	msg = timePattern.ReplaceAllString(msg, "<time>")
	return fmt.Sprintf("%s|%s", status.Code(err), msg)
}

package protorequire

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Sentinel values — strings use a distinctive value unlikely to appear in real
// data; messages are identified by pointer address; integers use magic values.
var (
	nonZeroString    = "__protorequire_NonZero_sentinel__"
	nonZeroTimestamp = &timestamppb.Timestamp{}
	nonZeroDuration  = &durationpb.Duration{}
	// Arbitrary large negatives — not powers of 2 or boundary values,
	// so they won't collide with real test data or edge-case tests.
	nonZeroInt32 = int32(-479_001_599)
	nonZeroInt64 = int64(-6_279_541_637_813)
)

// NonZero returns a sentinel value that can be used as a field value in an
// expected proto message. When [ProtoEqual] encounters this sentinel, it
// asserts that the corresponding field in the actual message is present and
// non-zero, rather than checking for exact equality.
//
// Example:
//
//	expected := &pb.MyMessage{
//	    Name:       "exact match",
//	    CreatedAt:  protorequire.NonZero[*timestamppb.Timestamp](),
//	    ExternalId: protorequire.NonZero[string](),
//	    Count:      protorequire.NonZero[int64](),
//	    Duration:   protorequire.NonZero[*durationpb.Duration](),
//	}
//	protorequire.ProtoEqual(t, expected, actual)
func NonZero[T string | *timestamppb.Timestamp | *durationpb.Duration | int32 | int64]() T {
	var zero T
	switch any(zero).(type) {
	case string:
		return any(nonZeroString).(T)
	case *timestamppb.Timestamp:
		return any(nonZeroTimestamp).(T)
	case *durationpb.Duration:
		return any(nonZeroDuration).(T)
	case int32:
		return any(nonZeroInt32).(T)
	case int64:
		return any(nonZeroInt64).(T)
	}
	panic("unreachable")
}

func isMessageSentinel(m protoreflect.Message) bool {
	switch v := m.Interface().(type) {
	case *timestamppb.Timestamp:
		return v == nonZeroTimestamp
	case *durationpb.Duration:
		return v == nonZeroDuration
	}
	return false
}

// fieldPath is a chain of field descriptors from root to the sentinel field.
type fieldPath []protoreflect.FieldDescriptor

func (p fieldPath) String() string {
	var b strings.Builder
	for i, fd := range p {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(string(fd.Name()))
	}
	return b.String()
}

// findSentinelPaths walks the original (uncloned) message to find sentinel
// values using pointer identity (for strings and messages) or magic values
// (for integers). Returns the path to each sentinel field.
func findSentinelPaths(msg protoreflect.Message) []fieldPath {
	return findSentinelPathsRecurse(msg, nil)
}

func findSentinelPathsRecurse(msg protoreflect.Message, prefix fieldPath) []fieldPath {
	var paths []fieldPath
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		current := append(append(fieldPath(nil), prefix...), fd)
		switch fd.Kind() {
		case protoreflect.StringKind:
			if v.String() == nonZeroString {
				paths = append(paths, current)
			}
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			if int32(v.Int()) == nonZeroInt32 {
				paths = append(paths, current)
			}
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			if v.Int() == nonZeroInt64 {
				paths = append(paths, current)
			}
		case protoreflect.MessageKind:
			if isMessageSentinel(v.Message()) {
				paths = append(paths, current)
			} else {
				paths = append(paths, findSentinelPathsRecurse(v.Message(), current)...)
			}
		}
		return true
	})
	return paths
}

// validateSentinel checks that the actual message has the field at path set
// to a non-zero value. Uses read-only traversal to avoid mutating actual.
func validateSentinel(actual proto.Message, path fieldPath) bool {
	msg := actual.ProtoReflect()
	for _, fd := range path[:len(path)-1] {
		if !msg.Has(fd) {
			return false
		}
		msg = msg.Get(fd).Message()
	}
	return msg.Has(path[len(path)-1])
}

// clearField clears the leaf field at path in the given message.
// Uses Mutable to traverse — only safe on cloned messages.
func clearField(msg proto.Message, path fieldPath) {
	m := msg.ProtoReflect()
	for _, fd := range path[:len(path)-1] {
		m = m.Mutable(fd).Message()
	}
	m.Clear(path[len(path)-1])
}

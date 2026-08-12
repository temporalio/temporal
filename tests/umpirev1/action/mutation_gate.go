package action

import (
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The mutation analogue of ValidateKitchensinkMappings. Where kitchensink completeness is
// per-action ("every WorkerCommand action has a realization"), invalid-input completeness is
// per-*field*: the negative-space claim (UMPIRE_ERR.md §0 — "the variant set falls out of the
// descriptor, not hand authoring") only holds if every request field is either enumerated by
// reflection or consciously deferred. This gate makes that checkable before any test runs, so a
// new request field of an unhandled kind can't be silently left untested.

// deferredMutationKinds are scalar proto field kinds whose invalid-input domains are not yet
// modeled (tracked as follow-ups in UMPIRE_ERR.md). A field of such a kind is out of scope on
// purpose; a covered kind (string / Duration) must be enumerated; anything else is a gap.
var deferredMutationKinds = map[protoreflect.Kind]bool{
	protoreflect.EnumKind:     true,
	protoreflect.Int32Kind:    true,
	protoreflect.Int64Kind:    true,
	protoreflect.Uint32Kind:   true,
	protoreflect.Uint64Kind:   true,
	protoreflect.Sint32Kind:   true,
	protoreflect.Sint64Kind:   true,
	protoreflect.Fixed32Kind:  true,
	protoreflect.Fixed64Kind:  true,
	protoreflect.Sfixed32Kind: true,
	protoreflect.Sfixed64Kind: true,
	protoreflect.BoolKind:     true,
	protoreflect.BytesKind:    true,
	protoreflect.FloatKind:    true,
	protoreflect.DoubleKind:   true,
}

// ValidateMutationCoverage checks that the invalid-input enumeration is exhaustive over each
// mutation-covered request descriptor: every scalar field is either enumerated (reflection
// produced a Domain for it — string / Duration today) or of a consciously-deferred kind. A
// covered-kind field reflection missed, or a field of an unclassified kind, is a gap — so a new
// request field can't be silently untested. Non-scalar messages (Payload, metadata) and
// collections are deferred as a whole.
func ValidateMutationCoverage() error {
	var problems []string
	for _, req := range mutationRequests() {
		covered := map[string]bool{}
		for _, p := range reflectStartParams(req) {
			covered[p.Path] = true
		}
		name := string(req.ProtoReflect().Descriptor().Name())
		for _, g := range fieldGaps(req, covered) {
			problems = append(problems, name+"."+g)
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("mutation coverage incomplete: %v", problems)
	}
	return nil
}

// mutationRequests are the request messages under invalid-input enumeration. One today; the gate
// generalizes as more entry-point RPCs gain mutation coverage.
func mutationRequests() []protoreflect.ProtoMessage {
	return []protoreflect.ProtoMessage{&workflowservice.StartNexusOperationExecutionRequest{}}
}

func fieldGaps(req protoreflect.ProtoMessage, covered map[string]bool) []string {
	var gaps []string
	fields := req.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.IsList() || fd.IsMap() {
			continue // collection-mutation domains are a separate follow-up
		}
		name := string(fd.Name())
		switch {
		case fd.Kind() == protoreflect.StringKind || isDurationField(fd):
			if !covered[name] {
				gaps = append(gaps, fmt.Sprintf("%s (%s: covered kind but not enumerated)", name, fd.Kind()))
			}
		case fd.Kind() == protoreflect.MessageKind:
			// Non-Duration messages (Payload, user metadata, search attributes) are deferred.
		case deferredMutationKinds[fd.Kind()]:
			// Consciously out of scope.
		default:
			gaps = append(gaps, fmt.Sprintf("%s (unclassified kind %s: add a domain or defer it)", name, fd.Kind()))
		}
	}
	return gaps
}

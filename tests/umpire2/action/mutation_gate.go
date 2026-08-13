package action

import (
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
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
// purpose; a covered kind (string / enum / Duration / Payload) must be enumerated; anything else
// is a gap.
var deferredMutationFields = map[string]string{
	"search_attributes": "search-attribute validation requires namespace schema state",
	"nexus_header":      "header collection mutations require request-level aggregate limits",
	"user_metadata":     "user-metadata limits are namespace-configured",
}

// ValidateMutationCoverage checks that the invalid-input enumeration is exhaustive over each
// mutation-covered request descriptor: every scalar field is either enumerated (reflection
// produced a Domain for it — string / enum / Duration / Payload today) or of a consciously-deferred
// kind. A covered-kind field reflection missed, or a field of an unclassified kind, is a gap — so
// a new request field can't be silently untested. Other non-scalar messages and collections are
// deferred as a whole.
func ValidateMutationCoverage() error {
	var problems []string
	for _, req := range mutationRequests() {
		covered := map[string]bool{}
		for _, p := range reflectStartParams(req) {
			_, unsupported := p.Domain.(*umpirefw.UnsupportedDomain)
			covered[p.Path] = !unsupported
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
		name := string(fd.Name())
		switch {
		case covered[name]:
			continue
		case deferredMutationFields[name] != "":
			continue
		default:
			gaps = append(gaps, fmt.Sprintf("%s (unclassified kind %s: register a validator or name a deferral)", name, fd.Kind()))
		}
	}
	return gaps
}

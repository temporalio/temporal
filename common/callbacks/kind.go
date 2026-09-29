package callbacks

import (
	"fmt"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/dynamicconfig"
)

// Kind identifies a callback variant.
type Kind string

const (
	// KindUnknown is the kind of a callback with an unset or unrecognized variant.
	KindUnknown      Kind = "unknown"
	KindNexus        Kind = "nexus"
	KindNexusHandler Kind = "nexusHandler"
)

func (k Kind) String() string {
	switch k {
	case KindUnknown, KindNexus, KindNexusHandler:
		return string(k)
	default:
		return string(KindUnknown)
	}
}

// KindOf reports which [Kind] the given callback is.
func KindOf(cb *commonpb.Callback) Kind {
	switch cb.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		return KindNexus
	case *commonpb.Callback_NexusHandler_:
		return KindNexusHandler
	case *commonpb.Callback_Internal_:
		// Internal-variant callbacks are not used and should be removed entirely.
		return KindUnknown
	default:
		return KindUnknown
	}
}

// ConvertEnabledKinds converts a dynamic config value, a list of kind names into a []Kind.
//
// Returns an error and use the default config value if any callback kinds are unrecognized.
// An empty list not specifying any callback kinds is allowed.
func ConvertEnabledKinds(val any) ([]Kind, error) {
	names, err := dynamicconfig.ConvertStructure[[]string](nil)(val)
	if err != nil {
		return nil, err
	}

	enabledKinds := make([]Kind, 0, 2)
	configurableKinds := map[string]Kind{
		KindNexus.String():        KindNexus,
		KindNexusHandler.String(): KindNexusHandler,
	}
	var unknownNames []string
	for _, name := range names {
		kind, ok := configurableKinds[name]
		if !ok {
			unknownNames = append(unknownNames, name)
			continue
		}
		if !slices.Contains(enabledKinds, kind) {
			enabledKinds = append(enabledKinds, kind)
		}
	}
	if len(unknownNames) > 0 {
		return nil, fmt.Errorf(
			"%v does not match a known callback kind [nexus, nexusHandler]",
			unknownNames)
	}
	return enabledKinds, nil
}

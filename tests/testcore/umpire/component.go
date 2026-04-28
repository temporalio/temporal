package umpire

import (
	"fmt"
	"sort"
	"strings"
)

// Component is the unit of model wiring: one domain concept (Namespace,
// TaskQueue, NexusEndpoint, …) packaged with its behavior, observer, rules,
// RPC catalog, and default fault profile.
//
// Components are combined into a Components list, which the test setup
// validates and unpacks into the umpire's NewInterceptor / NewComposite
// calls. This lets each domain package own everything it contributes to a
// property test, while the framework enforces consistency across the set.
//
// Every field is optional. A typical "domain entity" component populates
// Behavior + Observer + ObservedMethods + Rules + RPCs + Faults; a pure
// observer component populates only Observer + ObservedMethods.
type Component struct {
	// Name identifies the component in diagnostics (e.g. "namespace"). Must
	// be non-empty and unique within a Components list.
	Name string

	// Behavior is the model component that contributes Actions, CheckInvariant,
	// and Cleanup. Nil for observe-only components.
	Behavior ModelBehavior

	// Observer is the umpire.Strategy this component installs into the
	// interceptor (typically server-side handlers that update the World).
	// Zero value means no observer.
	Observer Strategy

	// ObservedMethods lists the gRPC method names this component's Observer
	// handles. Used by Validate to ensure every observed method is also
	// declared in the catalog (some component's RPCs).
	ObservedMethods []string

	// Rules is the per-component RuleSet. Components.Rules() merges every
	// component's set into one.
	Rules *RuleSet

	// RPCs catalogs the gRPC methods this component owns. Components.Validate
	// rejects sets where two components claim the same method.
	RPCs []RPCSpec

	// Faults declares default fault profiles for methods this component owns.
	// Keys must match an RPC method declared in this or another component.
	Faults map[string]MethodFault
}

// Components is a list of Component values that the test composes into a
// running scenario. Use Validate before extracting parts via Behaviors,
// Strategies, Rules, RPCs, Faults to catch wiring mistakes early.
type Components []*Component

// Behaviors returns the non-nil ModelBehaviors in declaration order,
// suitable for NewComposite.
func (cs Components) Behaviors() []ModelBehavior {
	out := make([]ModelBehavior, 0, len(cs))
	for _, c := range cs {
		if c.Behavior != nil {
			out = append(out, c.Behavior)
		}
	}
	return out
}

// Strategies returns each component's Observer in declaration order,
// suitable for inclusion in NewInterceptor. Strategies with no Client and
// no Server callback are skipped.
func (cs Components) Strategies() []Strategy {
	out := make([]Strategy, 0, len(cs))
	for _, c := range cs {
		if c.Observer.Client == nil && c.Observer.Server == nil {
			continue
		}
		out = append(out, c.Observer)
	}
	return out
}

// Rules merges every component's RuleSet into a fresh combined set, with
// duplicate-name dedup applied by RuleSet.Merge.
func (cs Components) Rules() *RuleSet {
	merged := &RuleSet{}
	for _, c := range cs {
		merged.Merge(c.Rules)
	}
	return merged
}

// RPCs returns the union of every component's RPC specs, sorted by method
// name. Duplicate methods are reported by Validate; merge keeps first-seen.
func (cs Components) RPCs() []RPCSpec {
	seen := make(map[string]bool)
	var out []RPCSpec
	for _, c := range cs {
		for _, s := range c.RPCs {
			if seen[s.Method] {
				continue
			}
			seen[s.Method] = true
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Method < out[j].Method })
	return out
}

// Faults returns a single fault map merged from every component. Conflicts
// (same method declared in two components) are reported by Validate; merge
// keeps the first-seen entry.
func (cs Components) Faults() map[string]MethodFault {
	out := make(map[string]MethodFault)
	for _, c := range cs {
		for k, v := range c.Faults {
			if _, ok := out[k]; ok {
				continue
			}
			out[k] = v
		}
	}
	return out
}

// PopulateRegistry registers every component's RPC specs into r in
// canonical (sorted by method) order.
func (cs Components) PopulateRegistry(r *RPCRegistry) {
	for _, s := range cs.RPCs() {
		r.Register(s)
	}
}

// Validate reports consistency issues across the component set. Returns nil
// if the set is consistent. Checks:
//   - Names are non-empty and unique.
//   - No two components own the same RPC method.
//   - Every fault key references an owned RPC method.
//   - Every fault key is declared by exactly one component.
//   - Every observed method references an owned RPC method.
//
// Validate is intended to be called during test setup; failing to call it
// means the test silently runs with whatever the merge happens to produce
// for conflicts.
func (cs Components) Validate() error {
	var errs []string

	// Name uniqueness.
	nameCount := make(map[string]int)
	for i, c := range cs {
		if c.Name == "" {
			errs = append(errs, fmt.Sprintf("component[%d] has empty Name", i))
			continue
		}
		nameCount[c.Name]++
	}
	for name, count := range nameCount {
		if count > 1 {
			errs = append(errs, fmt.Sprintf("component name %q declared %d times", name, count))
		}
	}

	// RPC method ownership: at most one component per method.
	methodOwner := make(map[string]string)
	for _, c := range cs {
		for _, s := range c.RPCs {
			if prev, ok := methodOwner[s.Method]; ok {
				errs = append(errs, fmt.Sprintf("RPC %q claimed by components %q and %q", s.Method, prev, c.Name))
				continue
			}
			methodOwner[s.Method] = c.Name
		}
	}

	// Fault keys must exist in the merged catalog and be declared exactly
	// once.
	faultOwner := make(map[string]string)
	for _, c := range cs {
		for method := range c.Faults {
			if _, ok := methodOwner[method]; !ok {
				errs = append(errs, fmt.Sprintf("component %q declares fault for %q with no matching RPC in any component", c.Name, method))
			}
			if prev, ok := faultOwner[method]; ok {
				errs = append(errs, fmt.Sprintf("fault for %q declared by both %q and %q", method, prev, c.Name))
				continue
			}
			faultOwner[method] = c.Name
		}
	}

	// Observed methods must exist in the catalog.
	for _, c := range cs {
		for _, method := range c.ObservedMethods {
			if _, ok := methodOwner[method]; !ok {
				errs = append(errs, fmt.Sprintf("component %q observes %q with no matching RPC in any component", c.Name, method))
			}
		}
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("umpire.Components.Validate:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

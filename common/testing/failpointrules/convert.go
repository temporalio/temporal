// Package failpointrules configures the failpoints of a server, if the startup switch enabled
// failpoints, from the testing.failpointRules dynamic config setting.
package failpointrules

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ruleConfig is a rule as it is written in dynamic config.
type ruleConfig struct {
	Key   string
	Scope string
	// Namespace is the name of the namespace that the rule applies to, as an alternative to Scope.
	Namespace string
	Action    string
	Error     string
	Message   string
	Delay     time.Duration
	Trigger   failpoint.Trigger
}

// ruleActions are the actions that rules can name, by the name of the action.
var ruleActions = map[string]failpoint.Action{
	"Return": failpoint.ActionReturn,
	"Panic":  failpoint.ActionPanic,
	"Block":  failpoint.ActionBlock,
	"Pause":  failpoint.ActionPause,
	"On":     failpoint.ActionOn,
}

// ConvertRules converts a dynamic config value to failpoint rules. The value is a list of maps
// with the fields key, scope, namespace, action, error, message, delay and trigger
// (probability, seed, skipFirst, maxFires). A rule has a scope, the ID of a namespace, or a
// namespace, its name, which applies to the gRPC and HTTP keys only (see failpointadapter.NameScope).
// A namespace that is empty is an error, because the rule would apply to every namespace. It returns
// an error if a field is unknown or malformed, or if the rules are not valid for failpoint.RuleSet.Apply.
func ConvertRules(value any) ([]failpoint.Rule, error) {
	if rules, ok := value.([]failpoint.Rule); ok {
		return rules, validateRules(rules)
	}
	configs, err := decodeRuleConfigs(value)
	if err != nil {
		return nil, err
	}
	rules := make([]failpoint.Rule, 0, len(configs))
	for i, c := range configs {
		rule, err := c.rule()
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}
		rules = append(rules, rule)
	}
	if err := validateRules(rules); err != nil {
		return nil, err
	}
	return rules, nil
}

func decodeRuleConfigs(value any) ([]ruleConfig, error) {
	if value == nil {
		return nil, nil
	}
	var rawRules []any
	switch v := value.(type) {
	case []any:
		rawRules = v
	case []map[string]any:
		rawRules = make([]any, len(v))
		for i, m := range v {
			rawRules[i] = m
		}
	case []map[any]any:
		rawRules = make([]any, len(v))
		for i, m := range v {
			rawRules[i] = m
		}
	default:
		return nil, fmt.Errorf("source data must be an array or slice, got %T", value)
	}
	configs := make([]ruleConfig, 0, len(rawRules))
	for i, raw := range rawRules {
		cfg, err := decodeRuleConfig(raw)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

func toStringMap(v any) (map[string]any, error) {
	switch m := v.(type) {
	case map[string]any:
		return m, nil
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, val := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("key %v is not a string", k)
			}
			out[ks] = val
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected a map, got %T", v)
	}
}

func decodeRuleConfig(raw any) (ruleConfig, error) {
	fields, err := toStringMap(raw)
	if err != nil {
		return ruleConfig{}, err
	}
	var cfg ruleConfig
	var unknown []string
	for name, val := range fields {
		switch strings.ToLower(name) {
		case "key":
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			cfg.Key = s
		case "scope":
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			cfg.Scope = s
		case "namespace":
			if val == nil {
				return ruleConfig{}, errors.New("namespace is empty; omit it for a rule that applies to every namespace")
			}
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			if s == "" {
				return ruleConfig{}, errors.New("namespace is empty; omit it for a rule that applies to every namespace")
			}
			cfg.Namespace = s
		case "action":
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			cfg.Action = s
		case "error":
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			cfg.Error = s
		case "message":
			s, err := decodeString(name, val)
			if err != nil {
				return ruleConfig{}, err
			}
			cfg.Message = s
		case "delay":
			d, err := decodeDuration(val)
			if err != nil {
				return ruleConfig{}, fmt.Errorf("delay: %w", err)
			}
			cfg.Delay = d
		case "trigger":
			tr, err := decodeTrigger(val)
			if err != nil {
				return ruleConfig{}, fmt.Errorf("trigger: %w", err)
			}
			cfg.Trigger = tr
		default:
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		return ruleConfig{}, fmt.Errorf("invalid keys: %s", strings.Join(unknown, ", "))
	}
	return cfg, nil
}

func decodeTrigger(raw any) (failpoint.Trigger, error) {
	if raw == nil {
		return failpoint.Trigger{}, nil
	}
	fields, err := toStringMap(raw)
	if err != nil {
		return failpoint.Trigger{}, err
	}
	var tr failpoint.Trigger
	var unknown []string
	for name, val := range fields {
		switch strings.ToLower(name) {
		case "probability":
			p, err := decodeFloat64(name, val)
			if err != nil {
				return failpoint.Trigger{}, err
			}
			tr.Probability = p
		case "seed":
			n, err := decodeInt64(name, val)
			if err != nil {
				return failpoint.Trigger{}, err
			}
			tr.Seed = n
		case "skipfirst":
			n, err := decodeInt64(name, val)
			if err != nil {
				return failpoint.Trigger{}, err
			}
			tr.SkipFirst = n
		case "maxfires":
			n, err := decodeInt64(name, val)
			if err != nil {
				return failpoint.Trigger{}, err
			}
			tr.MaxFires = n
		default:
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		return failpoint.Trigger{}, fmt.Errorf("invalid keys: %s", strings.Join(unknown, ", "))
	}
	return tr, nil
}

func decodeString(field string, val any) (string, error) {
	if val == nil {
		return "", nil
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s: expected type 'string', got %T", field, val)
	}
	return s, nil
}

func decodeFloat64(field string, val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("%s: expected type 'float64', got %T", field, val)
	}
}

func decodeInt64(field string, val any) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v || v < math.MinInt64 || v >= math.MaxInt64 {
			return 0, fmt.Errorf("%s: %v is not an integer", field, v)
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("%s: expected type 'int64', got %T", field, val)
	}
}

func decodeDuration(val any) (time.Duration, error) {
	switch v := val.(type) {
	case nil:
		return 0, nil
	case time.Duration:
		return v, nil
	case string:
		return time.ParseDuration(v)
	case int:
		return time.Duration(v) * time.Second, nil
	case int64:
		return time.Duration(v) * time.Second, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid duration %v", v)
		}
		return time.Duration(v * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("expected duration string or seconds, got %T", val)
	}
}

// validateRules checks that the rules are valid for failpoint.RuleSet.Apply, and that the gRPC keys
// name a unary method of a gRPC service in its full form, so that a typo doesn't create a failpoint
// that no call executes.
func validateRules(rules []failpoint.Rule) error {
	if err := failpoint.ValidateRules(rules); err != nil {
		return err
	}
	for _, r := range rules {
		if method, ok := serverfailpoints.GRPCMethod(r.Key); ok && !isUnaryGRPCMethod(method) {
			return fmt.Errorf("failpoint rule %s: %q is not the full name of a unary method of a gRPC service, for example %s",
				r.Key, method, "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution")
		}
	}
	return nil
}

// isUnaryGRPCMethod reports whether method is a unary method of a gRPC service that is linked into
// the binary, in the form "/pkg.Service/Method" that the gRPC interceptor executes failpoints
// with. Streaming methods are not intercepted, so no call would execute their failpoints.
func isUnaryGRPCMethod(method string) bool {
	path, ok := strings.CutPrefix(method, "/")
	if !ok {
		return false
	}
	service, name, _ := strings.Cut(path, "/")
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return false
	}
	serviceDesc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return false
	}
	methodDesc := serviceDesc.Methods().ByName(protoreflect.Name(name))
	return methodDesc != nil && !methodDesc.IsStreamingClient() && !methodDesc.IsStreamingServer()
}

func (c ruleConfig) rule() (failpoint.Rule, error) {
	action, err := lookupName(ruleActions, "action", c.Action)
	if err != nil {
		return failpoint.Rule{}, err
	}
	scope := c.Scope
	if c.Namespace != "" {
		// A name needs no namespace lookup when the rules are applied: the adapter of the
		// transport matches it when a request executes, so a namespace that is created later or
		// recreated is matched too.
		if c.Scope != "" {
			return failpoint.Rule{}, fmt.Errorf("rule for %s has a scope and a namespace; use one of them", c.Key)
		}
		if !serverfailpoints.IsTransportKey(c.Key) {
			return failpoint.Rule{}, fmt.Errorf("rule for %s has a namespace, which only applies to gRPC and HTTP failpoints (keys with the prefix grpc:, grpc-response:, http: or http-response:, or the keys of any operation); use scope with the namespace ID", c.Key)
		}
		scope = failpointadapter.NameScope(namespace.Name(c.Namespace))
	}
	return failpoint.Rule{
		Key:     c.Key,
		Scope:   scope,
		Action:  action,
		Error:   c.Error,
		Message: c.Message,
		Delay:   c.Delay,
		Trigger: c.Trigger,
	}, nil
}

// lookupName returns the value that values has for name, ignoring case. The error lists the names.
func lookupName[T any](values map[string]T, what, name string) (T, error) {
	for candidate, value := range values {
		if strings.EqualFold(candidate, name) {
			return value, nil
		}
	}
	var zero T
	return zero, fmt.Errorf("unknown %s %q, valid %ss are %s", what, name, what, strings.Join(slices.Sorted(maps.Keys(values)), ", "))
}

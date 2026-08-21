package callback

import (
	"context"
	"fmt"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc/status"
)

// Kind identifies a callback variant.
type Kind string

const (
	// KindUnknown is the kind of a callback with an unset or unrecognized variant.
	KindUnknown Kind = "unknown"
	KindNexus   Kind = "nexus"
	KindWorker  Kind = "worker"
)

func (k Kind) String() string {
	switch k {
	case KindUnknown, KindNexus, KindWorker:
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
	case *commonpb.Callback_Worker_:
		return KindWorker
	case *commonpb.Callback_Internal_:
		// Internal-variant callbacks are not used and should be removed entirely.
		return KindUnknown
	default:
		return KindUnknown
	}
}

// ConvertEnabledKinds converts a dynamic config value — a list of kind names as spelled by
// Kind.String() — into the []Kind it denotes.
//
// Returns an error and use the default config value if _any_ callback kinds are unrecognized.
// An empty list not specifying any callback kinds is allowed.
func ConvertEnabledKinds(val any) ([]Kind, error) {
	names, err := dynamicconfig.ConvertStructure[[]string](nil)(val)
	if err != nil {
		return nil, err
	}

	enabledKinds := make([]Kind, 0, 2)
	configurableKinds := map[string]Kind{
		KindNexus.String():  KindNexus,
		KindWorker.String(): KindWorker,
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
			"%v does not match a known callback kind [nexus, worker]",
			unknownNames)
	}
	return enabledKinds, nil
}

// Validator validates completion callbacks attached to executions (e.g. workflows and standalone activities).
type Validator interface {
	// Validate rejects callbacks that are not enabled for the execution, or are malformed.
	// Will mutate the supplied Callbacks to normalize. e.g. converting Nexus headers to lower-case.
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback, opts ValidateOptions) error
}

type ValidateOptions struct {
	// EnabledKinds are the callback kinds that may be attached to the execution being validated.
	// A client-supplied callback of any other kind is rejected with an InvalidArgument error.
	EnabledKinds []Kind
}

// OnlyNexus returns a ValidateOptions that only enables Nexus-variant callbacks. This is the default
// behavior for all execution types except for those on standalone Nexus operations, which may
// support Worker-variant callbacks.
func OnlyNexus() ValidateOptions {
	return ValidateOptions{
		EnabledKinds: []Kind{KindNexus},
	}
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	MaxPerExecution  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit dynamicconfig.IntPropertyFn // All ID types use the same global setting.
	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]
	// Worker-variant limits.
	MaxServiceNameLength       dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength     dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkerSourceContextMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

type validator struct {
	config ValidatorConfig
}

func NewValidator(config ValidatorConfig) Validator {
	return &validator{config: config}
}

// Validate validates completion callbacks: their kind, their count, and the fields of each variant.
// Nexus header keys are normalized to lowercase in place.
func (v *validator) Validate(
	_ context.Context,
	namespaceName string,
	cbs []*commonpb.Callback,
	opts ValidateOptions,
) error {
	if len(cbs) > v.config.MaxPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.config.MaxPerExecution(namespaceName),
		)
	}

	for i, cb := range cbs {
		if err := v.validateCallback(cb, namespaceName, opts); err != nil {
			return fmt.Errorf("completion_callbacks[%d]: %w", i, err)
		}
	}
	return nil
}

func (v *validator) validateCallback(cb *commonpb.Callback, namespaceName string, opts ValidateOptions) error {
	kind := KindOf(cb)

	// For unknown callbacks, prefer the "unknown callback variant" error below.
	if kind != KindUnknown && !slices.Contains(opts.EnabledKinds, kind) {
		return serviceerror.NewInvalidArgumentf("%s callbacks are not enabled for this execution type", kind)
	}

	switch kind {
	case KindNexus:
		return v.validateNexus(namespaceName, cb.GetNexus())
	case KindWorker:
		return v.validateWorker(namespaceName, cb.GetWorker())
	case KindUnknown:
		fallthrough
	default:
		return serviceerror.NewUnimplementedf("unknown callback variant: %T", cb.GetVariant())
	}
}

func (v *validator) validateNexus(namespaceName string, cb *commonpb.Callback_Nexus) error {
	rawURL := cb.GetUrl()
	if len(rawURL) > v.config.URLMaxLength(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"invalid url: url length longer than max length allowed of %d",
			v.config.URLMaxLength(namespaceName),
		)
	}
	if err := v.config.EndpointRules(namespaceName).Validate(rawURL); err != nil {
		msg := err.Error()
		if s, ok := status.FromError(err); ok {
			msg = s.Message()
		}
		return serviceerror.NewInvalidArgument(msg)
	}

	// Validate total size of all headers, as well as normalize to lowercase.
	headerSize := 0
	lowerCaseHeaders := make(map[string]string, len(cb.GetHeader()))
	for k, val := range cb.GetHeader() {
		headerSize += len(k) + len(val)
		lowerCaseHeaders[strings.ToLower(k)] = val
	}
	if headerSize > v.config.HeaderMaxSize(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"invalid header: header size longer than max allowed size of %d",
			v.config.HeaderMaxSize(namespaceName),
		)
	}
	cb.Header = lowerCaseHeaders
	return nil
}

func (v *validator) validateWorker(namespaceName string, cb *commonpb.Callback_Worker) error {
	if err := tqid.Validate(cb.GetTaskQueueName(), v.config.MaxIDLengthLimit()); err != nil {
		return err
	}

	for _, field := range []struct {
		name      string
		value     string
		maxLength int
	}{
		{"service", cb.GetService(), v.config.MaxServiceNameLength(namespaceName)},
		{"operation", cb.GetOperation(), v.config.MaxOperationNameLength(namespaceName)},
	} {
		if field.value == "" {
			return serviceerror.NewInvalidArgumentf("%s is required", field.name)
		}
		if len(field.value) > field.maxLength {
			return serviceerror.NewInvalidArgumentf(
				"%s exceeds length limit. Length=%d Limit=%d",
				field.name, len(field.value), field.maxLength)
		}
	}

	// Max size for Worker callback source context blobs.
	maxSize := v.config.WorkerSourceContextMaxSize(namespaceName)
	if size := cb.GetSourceContext().Size(); size > maxSize {
		return serviceerror.NewInvalidArgumentf(
			"source_context exceeds size limit. Length=%d Limit=%d",
			size, v.config.WorkerSourceContextMaxSize(namespaceName))
	}

	return nil
}

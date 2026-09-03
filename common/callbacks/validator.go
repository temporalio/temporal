package callbacks

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

type ValidatorOptions struct {
	// EnabledKinds are the callback kinds that may be attached to the execution being validated.
	// A client-supplied callback of any other kind is rejected with an InvalidArgument error.
	EnabledKinds []Kind
}

// AdditionOptions describes the completion callbacks an execution already carries, for
// [Validator.ValidateAdditions]. Callers read it off the execution's own state.
type AdditionOptions struct {
	// CurrentCallbacksAttached is the number of callbacks the execution already carries.
	CurrentCallbacksAttached int
	// CurrentTotalNexusHandlerCallbackSourceContextSize is the total bytes of NexusHandler source
	// context those callbacks carry between them.
	CurrentTotalNexusHandlerCallbackSourceContextSize int
}

// Validator validates completion callbacks attached to executions (e.g. workflows and standalone activities).
type Validator interface {
	// Validate rejects callbacks that are not enabled for the execution, or are malformed. It also
	// bounds their count and the total bytes of NexusHandler source context they carry.
	// Will mutate the supplied Callbacks to normalize. e.g. converting Nexus headers to lower-case.
	//
	// This is for a request that starts an execution, where cbs is the complete set of callbacks the
	// execution will carry. Callbacks attached to an execution that already holds some go through
	// [Validator.ValidateAdditions].
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback, opts ValidatorOptions) error

	// ValidateAdditions applies the count and aggregate source context size limits to cbs combined
	// with the callbacks the execution already carries. The frontend bounds only the callbacks on
	// one request, since it cannot see what a running execution already holds.
	//
	// It does not re-validate the individual callbacks; Validate has already done that at the
	// frontend. Unlike Validate, exceeding a limit here depends on execution state rather than the
	// request alone, so failures are FailedPrecondition rather than InvalidArgument.
	ValidateAdditions(namespaceName string, cbs []*commonpb.Callback, opts AdditionOptions) error
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	MaxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit         dynamicconfig.IntPropertyFn // All ID types use the same global setting.

	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]

	// NexusHandler-variant limits.
	MaxServiceNameLength                  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength                dynamicconfig.IntPropertyFnWithNamespaceFilter
	NexusHandlerSourceContextMaxSize      dynamicconfig.IntPropertyFnWithNamespaceFilter
	TotalNexusHandlerSourceContextMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (vc *ValidatorConfig) Validate() error {
	var missingFields []string
	assertGetterIsSet := func(name string, getter dynamicconfig.IntPropertyFnWithNamespaceFilter) {
		if getter == nil {
			missingFields = append(missingFields, name)
		}
	}

	assertGetterIsSet("MaxCallbacksPerExecution", vc.MaxCallbacksPerExecution)
	if vc.MaxIDLengthLimit == nil {
		missingFields = append(missingFields, "MaxIDLengthLimit")
	}

	assertGetterIsSet("URLMaxLength", vc.URLMaxLength)
	assertGetterIsSet("HeaderMaxSize", vc.HeaderMaxSize)
	if vc.EndpointRules == nil {
		missingFields = append(missingFields, "EndpointRules")
	}

	assertGetterIsSet("MaxServiceNameLength", vc.MaxServiceNameLength)
	assertGetterIsSet("MaxOperationNameLength", vc.MaxOperationNameLength)
	assertGetterIsSet("NexusHandlerSourceContextMaxSize", vc.NexusHandlerSourceContextMaxSize)
	assertGetterIsSet("TotalNexusHandlerSourceContextMaxSize", vc.TotalNexusHandlerSourceContextMaxSize)

	if len(missingFields) != 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}
	return nil
}

type validator struct {
	config ValidatorConfig
}

// NewValidator returns a new Validator.
func NewValidator(config ValidatorConfig) (Validator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &validator{config: config}, nil
}

// Validate validates completion callbacks: their kind, their count, the fields of each variant, and
// the total NexusHandler source context they carry. Nexus header keys are normalized to lowercase in
// place.
func (v *validator) Validate(
	_ context.Context,
	namespaceName string,
	cbs []*commonpb.Callback,
	opts ValidatorOptions,
) error {
	if maxCount := v.config.MaxCallbacksPerExecution(namespaceName); len(cbs) > maxCount {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", maxCount,
		)
	}

	for i, cb := range cbs {
		if err := v.validateCallback(cb, namespaceName, opts); err != nil {
			return fmt.Errorf("completion_callbacks[%d]: %w", i, err)
		}
	}

	// validateNexusHandler bounds each callback's source context individually; this bounds the total.
	return v.validateSourceContextSize(namespaceName, sumNexusHandlerSourceContextSize(cbs))
}

// ValidateAdditions bounds cbs against what the execution already carries. See [Validator].
func (v *validator) ValidateAdditions(
	namespaceName string,
	cbs []*commonpb.Callback,
	opts AdditionOptions,
) error {
	maxCount := v.config.MaxCallbacksPerExecution(namespaceName)
	if len(cbs)+opts.CurrentCallbacksAttached > maxCount {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to an execution (%d callbacks already attached)",
			maxCount, opts.CurrentCallbacksAttached,
		)
	}
	return v.validateSourceContextSize(
		namespaceName,
		sumNexusHandlerSourceContextSize(cbs)+opts.CurrentTotalNexusHandlerCallbackSourceContextSize,
	)
}

// validateSourceContextSize bounds the total bytes of NexusHandler source context an execution would
// carry against the per-execution limit.
func (v *validator) validateSourceContextSize(namespaceName string, bytes int) error {
	if maxSize := v.config.TotalNexusHandlerSourceContextMaxSize(namespaceName); bytes > maxSize {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d bytes of callback source_context to an execution (%d bytes requested)",
			maxSize, bytes)
	}
	return nil
}

func (v *validator) validateCallback(cb *commonpb.Callback, namespaceName string, opts ValidatorOptions) error {
	kind := KindOf(cb)

	// For unknown callbacks, prefer the "unknown callback variant" error below.
	if kind != KindUnknown && !slices.Contains(opts.EnabledKinds, kind) {
		return serviceerror.NewInvalidArgumentf("%s callbacks are not enabled for this execution type", kind)
	}

	switch kind {
	case KindNexus:
		return v.validateNexus(namespaceName, cb.GetNexus())
	case KindNexusHandler:
		return v.validateNexusHandler(namespaceName, cb.GetNexusHandler())
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

func (v *validator) validateNexusHandler(namespaceName string, cb *commonpb.Callback_NexusHandler) error {
	// Task Queue
	if err := tqid.Validate(cb.GetTaskQueueName(), v.config.MaxIDLengthLimit()); err != nil {
		return err
	}

	// Nexus handler
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

	// Source Context blob
	maxSize := v.config.NexusHandlerSourceContextMaxSize(namespaceName)
	if size := cb.GetSourceContext().Size(); size > maxSize {
		return serviceerror.NewInvalidArgumentf(
			"source_context exceeds size limit. Length=%d Limit=%d",
			size, v.config.NexusHandlerSourceContextMaxSize(namespaceName))
	}

	return nil
}

// sumNexusHandlerSourceContextSize returns the total size in bytes of the NexusHandler source context payloads carried
// by cbs. Callbacks of any other kind contribute nothing.
func sumNexusHandlerSourceContextSize(cbs []*commonpb.Callback) int {
	total := 0
	for _, cb := range cbs {
		if sc := cb.GetNexusHandler().GetSourceContext(); sc != nil {
			total += sc.Size()
		}
	}
	return total
}

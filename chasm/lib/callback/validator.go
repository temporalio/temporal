package callback

import (
	"context"
	"fmt"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc/status"
)

// CallbackKind identifies a callback variant.
type CallbackKind int

const (
	// CallbackKindUnspecified is the kind of a callback with an unset or unrecognized variant.
	CallbackKindUnspecified CallbackKind = iota
	CallbackKindNexus
	CallbackKindWorker
	CallbackKindInternal
)

func (k CallbackKind) String() string {
	switch k {
	case CallbackKindNexus:
		return "nexus"
	case CallbackKindWorker:
		return "worker"
	case CallbackKindInternal:
		return "internal"
	default:
		return "unspecified"
	}
}

// KindOf reports which [CallbackKind] the given callback is.
func KindOf(cb *commonpb.Callback) CallbackKind {
	switch cb.GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		return CallbackKindNexus
	case *commonpb.Callback_Worker_:
		return CallbackKindWorker
	case *commonpb.Callback_Internal_:
		return CallbackKindInternal
	default:
		return CallbackKindUnspecified
	}
}

// EnabledCallbackKinds is the set of client-supplied callback kinds that may be attached to an execution.
// (e.g. Worker-variant callbacks might only be supported for SANO.)
type EnabledCallbackKinds []CallbackKind

func (e EnabledCallbackKinds) Contains(k CallbackKind) bool {
	return slices.Contains(e, k)
}

func (e EnabledCallbackKinds) String() string {
	names := make([]string, len(e))
	for i, kind := range e {
		names[i] = kind.String()
	}
	return strings.Join(names, ",")
}

// ConvertEnabledKinds converts a dynamic config value — a list of kind names as spelled by
// [CallbackKind.String] — into the [EnabledCallbackKinds] it denotes. Names are matched
// case-insensitively, and duplicate and unrecognized names are dropped: a config that names a kind
// this server is too old to know about still enables the kinds listed beside it.
//
// A value that names no recognized kind at all — an empty list, or a list of nothing but
// unrecognized names — is treated as an error rather than as "enable nothing".
func ConvertEnabledKinds(val any) (EnabledCallbackKinds, error) {
	names, err := dynamicconfig.ConvertStructure[[]string](nil)(val)
	if err != nil {
		return nil, err
	}

	// The kinds an operator may name. Internal callbacks are server-generated, so they are always
	// allowed and cannot be configured.
	configurableKinds := map[string]CallbackKind{
		CallbackKindNexus.String():  CallbackKindNexus,
		CallbackKindWorker.String(): CallbackKindWorker,
	}

	var enabledKinds EnabledCallbackKinds
	for _, name := range names {
		kind, ok := configurableKinds[strings.ToLower(strings.TrimSpace(name))]
		if ok && !enabledKinds.Contains(kind) {
			enabledKinds = append(enabledKinds, kind)
		}
	}
	if len(enabledKinds) == 0 {
		return nil, fmt.Errorf(
			"%v does not name any known callback kind, expected a non-empty subset of [nexus, worker]", names)
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
	// A client-supplied callback of any other kind is rejected with an Unimplemented error.
	EnabledKinds EnabledCallbackKinds
}

// ValidatorConfig holds the limits a [Validator] enforces. Every field is required.
type ValidatorConfig struct {
	MaxPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]
	// Worker-variant limits.
	WorkerNameMaxLength        dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkerSourceContextMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// NewValidatorConfig builds the production [ValidatorConfig] from dynamic config.
func NewValidatorConfig(dc *dynamicconfig.Collection) ValidatorConfig {
	return ValidatorConfig{
		MaxPerExecution:            MaxPerExecution.Get(dc),
		URLMaxLength:               dynamicconfig.FrontendCallbackURLMaxLength.Get(dc),
		HeaderMaxSize:              dynamicconfig.FrontendCallbackHeaderMaxSize.Get(dc),
		EndpointRules:              AllowedAddresses.Get(dc),
		WorkerNameMaxLength:        WorkerNameMaxLength.Get(dc),
		WorkerSourceContextMaxSize: WorkerSourceContextMaxSize.Get(dc),
	}
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
		kind := KindOf(cb)
		switch kind {
		case CallbackKindUnspecified:
			return serviceerror.NewUnimplementedf(
				"completion_callbacks[%d]: unknown callback variant: %T", i, cb.GetVariant())
		case CallbackKindInternal:
			// Internal callbacks are server-generated, so they are neither gated on operator
			// config nor have any fields to validate. CHASM has no Internal variant, so
			// FromAPICallback rejects them with InvalidArgument when the execution is backed by
			// CHASM.
			continue
		}

		if !opts.EnabledKinds.Contains(kind) {
			return serviceerror.NewUnimplementedf(
				"completion_callbacks[%d]: %s callbacks are not enabled for this execution type", i, kind)
		}

		var err error
		switch kind {
		case CallbackKindNexus:
			err = v.validateNexus(namespaceName, cb.GetNexus())
		case CallbackKindWorker:
			err = v.validateWorker(namespaceName, i, cb.GetWorker())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateNexus(namespaceName string, cb *commonpb.Callback_Nexus) error {
	rawURL := cb.GetUrl()
	if len(rawURL) > v.config.URLMaxLength(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"invalid url: url length longer than max length allowed of %d", v.config.URLMaxLength(namespaceName),
		)
	}
	if err := v.config.EndpointRules(namespaceName).Validate(rawURL); err != nil {
		if s, ok := status.FromError(err); ok {
			return serviceerror.NewInvalidArgument(s.Message())
		}
		return serviceerror.NewInvalidArgument(err.Error())
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
			"invalid header: header size longer than max allowed size of %d", v.config.HeaderMaxSize(namespaceName),
		)
	}
	cb.Header = lowerCaseHeaders
	return nil
}

func (v *validator) validateWorker(namespaceName string, idx int, cb *commonpb.Callback_Worker) error {
	nameLimit := v.config.WorkerNameMaxLength(namespaceName)
	for _, field := range []struct {
		name  string
		value string
	}{
		{"task_queue_name", cb.GetTaskQueueName()},
		{"service", cb.GetService()},
		{"operation", cb.GetOperation()},
	} {
		if field.value == "" {
			return serviceerror.NewInvalidArgumentf(
				"completion_callbacks[%d].worker.%s is required", idx, field.name)
		}
		if len(field.value) > nameLimit {
			return serviceerror.NewInvalidArgumentf(
				"completion_callbacks[%d].worker.%s exceeds length limit. Length=%d Limit=%d",
				idx, field.name, len(field.value), nameLimit)
		}
	}

	// Max size for Worker callback source context blobs.
	if size := cb.GetSourceContext().Size(); size > v.config.WorkerSourceContextMaxSize(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"completion_callbacks[%d].worker.source_context exceeds size limit. Length=%d Limit=%d",
			idx, size, v.config.WorkerSourceContextMaxSize(namespaceName))
	}
	return nil
}

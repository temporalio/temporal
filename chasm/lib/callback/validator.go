package callback

import (
	"context"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc/status"
)

// CallbackKind identifies a callback variant.
//
// Which kinds an execution accepts is a property of the execution type, not of the callback: only
// standalone Nexus operations can deliver a Worker callback today, and only the server itself
// attaches Internal ones. Callers declare what they can deliver via
// [Validator.VerifyOnlySupportedKinds], which keeps that decision at the call site while the
// field-level validation of every variant stays here.
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

// Validator validates completion callbacks attached to executions (workflows, standalone activities,
// and standalone Nexus operations).
type Validator interface {
	// Validate validates and normalizes the callbacks against the namespace's configured limits.
	//
	// It does not police which variants the caller is willing to deliver: a callback of any known
	// kind passes as long as it is well formed. Pair it with VerifyOnlySupportedKinds.
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback) error
	// VerifyOnlySupportedKinds rejects any callback whose kind is not in supported.
	VerifyOnlySupportedKinds(cbs []*commonpb.Callback, supported ...CallbackKind) error
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

// Validate validates completion callbacks: their count, and the fields of each variant. Nexus header
// keys are normalized to lowercase in place.
func (v *validator) Validate(_ context.Context, namespaceName string, cbs []*commonpb.Callback) error {
	if len(cbs) > v.config.MaxPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.config.MaxPerExecution(namespaceName),
		)
	}

	for i, cb := range cbs {
		switch variant := cb.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			if err := v.validateNexus(namespaceName, variant.Nexus); err != nil {
				return err
			}
		case *commonpb.Callback_Worker_:
			if err := v.validateWorker(namespaceName, i, variant.Worker); err != nil {
				return err
			}
		case *commonpb.Callback_Internal_:
			// Internal callbacks are server-generated, so there is nothing to validate.
			// CHASM has no Internal variant, so FromAPICallback rejects them with
			// InvalidArgument when the execution is backed by CHASM.
			continue
		default:
			return unknownVariantError(i, cb)
		}
	}
	return nil
}

func (v *validator) VerifyOnlySupportedKinds(cbs []*commonpb.Callback, supported ...CallbackKind) error {
	for i, cb := range cbs {
		kind := KindOf(cb)
		if kind == CallbackKindUnspecified {
			return unknownVariantError(i, cb)
		}
		if !slices.Contains(supported, kind) {
			return serviceerror.NewUnimplementedf(
				"completion_callbacks[%d]: %s callbacks are not supported for this execution type", i, kind)
		}
	}
	return nil
}

func unknownVariantError(idx int, cb *commonpb.Callback) error {
	return serviceerror.NewUnimplementedf(
		"completion_callbacks[%d]: unknown callback variant: %T", idx, cb.GetVariant())
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

// validateWorker checks a Worker-variant callback. The task queue, service, and operation together
// address the handler the completion is delivered to, so all three are required: without them the
// callback has nowhere to go, and no delivery attempt would change that.
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
	// The source context is opaque user data that the server carries to the handler untouched, so it
	// is bounded on the way in.
	if size := cb.GetSourceContext().Size(); size > v.config.WorkerSourceContextMaxSize(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"completion_callbacks[%d].worker.source_context exceeds size limit. Length=%d Limit=%d",
			idx, size, v.config.WorkerSourceContextMaxSize(namespaceName))
	}
	return nil
}

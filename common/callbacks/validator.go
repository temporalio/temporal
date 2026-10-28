package callbacks

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc/status"
)

type ValidatorOptions struct {
	// EnabledKinds are the callback kinds that may be attached to the execution being validated.
	// A client-supplied callback of any other kind is rejected with an InvalidArgument error.
	EnabledKinds []Kind
}

// CurrentCallbacksInfo describes the callbacks already attached to an execution, so that
// aggregate limits (max callbacks, total max size of callbacks) can be enforced.
type CurrentCallbacksInfo struct {
	Count     int
	TotalSize int // Size of all attached callbacks in bytes.
}

// Validator validates completion callbacks attached to executions (e.g. workflows and standalone activities).
type Validator interface {
	// Validate rejects callbacks that are not enabled for the execution, or are malformed.
	// Will mutate the supplied Callbacks to normalize. e.g. converting Nexus headers to lower-case.
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback, opts ValidatorOptions) error

	// ValidateAdditions enforces aggregate limits like the total number of callbacks that can be attached
	// to an execution. Relies on denormalized [CurrentCallbacksInfo] to avoid deserializing all existing
	// callbacks whenever a new one is added.
	//
	// This is the cumulative counterpart to Validate. And should always be called, even on Start- paths.
	// (Since the request to start an execution could potentially contain too many or too large of callbacks.)
	ValidateAdditions(namespaceName string, newCBs []*commonpb.Callback, existing CurrentCallbacksInfo) error
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	TotalCallbacksMaxSize    dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit         dynamicconfig.IntPropertyFn // All ID types use the same global setting.

	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]

	// NexusHandler-variant limits.
	MaxServiceNameLength             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength           dynamicconfig.IntPropertyFnWithNamespaceFilter
	NexusHandlerSourceContextMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (vc *ValidatorConfig) Validate() error {
	var missingFields []string
	assertGetterIsSet := func(name string, getter dynamicconfig.IntPropertyFnWithNamespaceFilter) {
		if getter == nil {
			missingFields = append(missingFields, name)
		}
	}

	assertGetterIsSet("TotalCallbacksMaxSize", vc.TotalCallbacksMaxSize)
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

	if len(missingFields) != 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}
	return nil
}

type validator struct {
	config            ValidatorConfig
	namespaceRegistry namespace.Registry
}

// NewValidator returns a new Validator.
func NewValidator(config ValidatorConfig, namespaceRegistry namespace.Registry) (Validator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &validator{config: config, namespaceRegistry: namespaceRegistry}, nil
}

// Validate validates completion callbacks: their kind, their count, and the fields of each variant.
// Nexus header keys are normalized to lowercase in place.
func (v *validator) Validate(
	_ context.Context,
	namespaceName string,
	cbs []*commonpb.Callback,
	opts ValidatorOptions,
) error {
	if len(cbs) > v.config.MaxCallbacksPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.config.MaxCallbacksPerExecution(namespaceName),
		)
	}

	for _, cb := range cbs {
		if err := v.validateCallback(cb, namespaceName, opts); err != nil {
			return err
		}
	}
	return nil
}

// ValidateAdditions checks the count and total size the execution would reach once newCBs are
// attached. Errors are FailedPrecondition rather than InvalidArgument: the request may be
// perfectly well-formed and only fail because of what the execution already holds.
func (v *validator) ValidateAdditions(
	namespaceName string,
	newCBs []*commonpb.Callback,
	existing CurrentCallbacksInfo,
) error {
	// Check aggregate callback count.
	maxCount := v.config.MaxCallbacksPerExecution(namespaceName)
	if existing.Count+len(newCBs) > maxCount {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to an execution (%d callbacks already attached)",
			maxCount,
			existing.Count,
		)
	}

	// Check aggregate callback size.
	var addedBytes int
	for _, cb := range newCBs {
		addedBytes += cb.Size()
	}
	maxSize := v.config.TotalCallbacksMaxSize(namespaceName)
	if existing.TotalSize+addedBytes > maxSize {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d bytes of callbacks to an execution "+
				"(%d bytes already attached, %d more requested)",
			maxSize,
			existing.TotalSize,
			addedBytes,
		)
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
	if rawURL == chasm.NexusCompletionHandlerURL {
		return v.validateInternalCallback(namespaceName, cb)
	}
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

func (v *validator) validateInternalCallback(namespaceName string, cb *commonpb.Callback_Nexus) error {
	token := nexus.Header(cb.GetHeader()).Get(commonnexus.CallbackTokenHeader)
	if token == "" {
		return serviceerror.NewInvalidArgument("missing internal callback token")
	}
	serializedRef, _, err := chasm.UnpackNexusCallbackToken(token)
	if err != nil {
		return serviceerror.NewInvalidArgumentf("invalid internal callback token: %v", err)
	}
	ref := &persistencespb.ChasmComponentRef{}
	if err := ref.Unmarshal(serializedRef); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid internal callback component reference: %v", err)
	}
	if ref.GetNamespaceId() == "" || ref.GetBusinessId() == "" {
		return serviceerror.NewInvalidArgument("internal callback component reference requires namespace and business IDs")
	}
	namespaceID, err := v.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return err
	}
	if ref.GetNamespaceId() != namespaceID.String() {
		return serviceerror.NewInvalidArgument("internal callback must target the same namespace")
	}
	return nil
}

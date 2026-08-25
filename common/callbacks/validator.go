package callbacks

import (
	"context"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc/status"
)

// Validator validates completion callbacks attached to executions (e.g. workflows and standalone activities).
type Validator interface {
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback) error
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	MaxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter

	// Nexus-variant limits.
	URLMaxLength  dynamicconfig.IntPropertyFnWithNamespaceFilter
	HeaderMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter
	EndpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]
}

func (vc *ValidatorConfig) Validate() error {
	var missingFields []string
	if vc.MaxCallbacksPerExecution == nil {
		missingFields = append(missingFields, "MaxCallbacksPerExecution")
	}
	if vc.URLMaxLength == nil {
		missingFields = append(missingFields, "URLMaxLength")
	}
	if vc.HeaderMaxSize == nil {
		missingFields = append(missingFields, "HeaderMaxSize")
	}
	if vc.EndpointRules == nil {
		missingFields = append(missingFields, "EndpointRules")
	}

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

// Validate validates completion callbacks: count, URL length, endpoint allowlist, header size, and normalizes header
// keys to lowercase.
func (v *validator) Validate(_ context.Context, namespaceName string, cbs []*commonpb.Callback) error {
	if len(cbs) > v.config.MaxCallbacksPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.config.MaxCallbacksPerExecution(namespaceName),
		)
	}

	for _, cb := range cbs {
		switch variant := cb.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			rawURL := variant.Nexus.GetUrl()
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
			lowerCaseHeaders := make(map[string]string, len(variant.Nexus.GetHeader()))
			for k, val := range variant.Nexus.GetHeader() {
				headerSize += len(k) + len(val)
				lowerCaseHeaders[strings.ToLower(k)] = val
			}
			if headerSize > v.config.HeaderMaxSize(namespaceName) {
				return serviceerror.NewInvalidArgumentf(
					"invalid header: header size longer than max allowed size of %d", v.config.HeaderMaxSize(namespaceName),
				)
			}
			variant.Nexus.Header = lowerCaseHeaders
		case *commonpb.Callback_Internal_:
			continue
		default:
			return serviceerror.NewUnimplemented(fmt.Sprintf("unknown callback variant: %T", variant))
		}
	}
	return nil
}

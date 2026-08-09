package callback

import (
	"context"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/grpc/status"
)

// Validator validates completion callbacks attached to executions (workflows and standalone activities).
type Validator interface {
	Validate(ctx context.Context, namespaceName string, cbs []*commonpb.Callback) error
}

type validator struct {
	maxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	urlMaxLength             dynamicconfig.IntPropertyFnWithNamespaceFilter
	headerMaxSize            dynamicconfig.IntPropertyFnWithNamespaceFilter
	endpointRules            dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]
}

func NewValidator(
	maxCallbacksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter,
	urlMaxLength dynamicconfig.IntPropertyFnWithNamespaceFilter,
	headerMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
	endpointRules dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules],
) Validator {
	return &validator{
		maxCallbacksPerExecution: maxCallbacksPerExecution,
		urlMaxLength:             urlMaxLength,
		headerMaxSize:            headerMaxSize,
		endpointRules:            endpointRules,
	}
}

// Validate validates completion callbacks: count, URL length, endpoint allowlist, header size, and normalizes header
// keys to lowercase.
func (v *validator) Validate(_ context.Context, namespaceName string, cbs []*commonpb.Callback) error {
	if len(cbs) > v.maxCallbacksPerExecution(namespaceName) {
		return serviceerror.NewInvalidArgumentf(
			"cannot attach more than %d callbacks to an execution", v.maxCallbacksPerExecution(namespaceName),
		)
	}

	for _, cb := range cbs {
		switch variant := cb.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			rawURL := variant.Nexus.GetUrl()
			if len(rawURL) > v.urlMaxLength(namespaceName) {
				return serviceerror.NewInvalidArgumentf(
					"invalid url: url length longer than max length allowed of %d", v.urlMaxLength(namespaceName),
				)
			}
			if err := v.endpointRules(namespaceName).Validate(rawURL); err != nil {
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
			if headerSize > v.headerMaxSize(namespaceName) {
				return serviceerror.NewInvalidArgumentf(
					"invalid header: header size longer than max allowed size of %d", v.headerMaxSize(namespaceName),
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

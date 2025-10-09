package interceptor

import (
	"context"
	"strings"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	ExperimentalFeaturesInterceptor struct {
		enabledExperiments dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string]
		namespaceRegistry  namespace.Registry
	}

	experimentalFeaturesContextKey struct{}
)

var _ grpc.UnaryServerInterceptor = (*ExperimentalFeaturesInterceptor)(nil).Intercept

func NewExperimentalFeaturesInterceptor(
	enabledExperiments dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string],
	namespaceRegistry namespace.Registry,
) *ExperimentalFeaturesInterceptor {
	return &ExperimentalFeaturesInterceptor{
		enabledExperiments: enabledExperiments,
		namespaceRegistry:  namespaceRegistry,
	}
}

func (i *ExperimentalFeaturesInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// Read all x-temporal-experimental header values
	experimentalHeaders := headers.GetValues(ctx, headers.ExperimentalHeaderName)

	if len(experimentalHeaders) > 0 && experimentalHeaders[0] != "" {
		// Get namespace name from the request
		namespaceName := GetNamespaceName(i.namespaceRegistry, req)

		// Get enabled experiments from dynamic config for this namespace
		enabledExperiments := i.enabledExperiments(namespaceName.String())

		// Collect all valid experiments
		var validExperiments []string

		// Check each header value (could be multiple header instances or comma-separated)
		for _, headerValue := range experimentalHeaders {
			if headerValue == "" {
				continue
			}
			// Split by comma in case multiple experiments are sent in one header
			requestedExperiments := strings.Split(headerValue, ",")
			for _, requested := range requestedExperiments {
				requested = strings.TrimSpace(requested)
				if requested == "" {
					continue
				}
				// Check if this experiment is enabled in dynamic config
				for _, enabled := range enabledExperiments {
					if strings.EqualFold(enabled, requested) {
						validExperiments = append(validExperiments, requested)
						break
					}
				}
			}
		}

		// Set all valid experiments in the context
		if len(validExperiments) > 0 {
			ctx = context.WithValue(ctx, experimentalFeaturesContextKey{}, validExperiments)
		}
	}

	return handler(ctx, req)
}

// IsExperimentEnabled checks if a specific experiment is enabled in the context
func IsExperimentEnabled(ctx context.Context, experimentName string) bool {
	if val := ctx.Value(experimentalFeaturesContextKey{}); val != nil {
		if experiments, ok := val.([]string); ok {
			for _, exp := range experiments {
				if strings.EqualFold(exp, experimentName) {
					return true
				}
			}
		}
	}
	return false
}

// GetEnabledExperiments returns all enabled experiments from the context
func GetEnabledExperiments(ctx context.Context) []string {
	if val := ctx.Value(experimentalFeaturesContextKey{}); val != nil {
		if experiments, ok := val.([]string); ok {
			return experiments
		}
	}
	return nil
}

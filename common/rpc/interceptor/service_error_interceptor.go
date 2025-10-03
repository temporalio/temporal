package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
)

type (
	// ServiceErrorInterceptor handles error transformation and pre-telemetry error metrics/logging
	ServiceErrorInterceptor struct {
		requestErrorHandler *RequestErrorHandler
		metricsHandler      metrics.Handler
		namespaceRegistry   namespace.Registry
	}
)

// NewServiceErrorInterceptor creates a new ServiceErrorInterceptor
func NewServiceErrorInterceptor(
	requestErrorHandler *RequestErrorHandler,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
) *ServiceErrorInterceptor {
	return &ServiceErrorInterceptor{
		requestErrorHandler: requestErrorHandler,
		metricsHandler:      metricsHandler,
		namespaceRegistry:   namespaceRegistry,
	}
}

func (i *ServiceErrorInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	resp, err := handler(ctx, req)

	if err != nil {
		// Check if this is a PreTelemetryError that needs metrics/logging
		var preTelemetryErr *PreTelemetryError
		if errors.As(err, &preTelemetryErr) {
			// Extract the original error for handling
			originalErr := preTelemetryErr.Cause

			// Get namespace and method info for metrics/logging
			methodName := info.FullMethod
			nsName := MustGetNamespaceName(i.namespaceRegistry, req)

			// Create metrics handler and log tags
			metricsHandler, logTags := CreateUnaryMetricsHandlerLogTags(
				i.metricsHandler,
				req,
				info.FullMethod,
				methodName,
				nsName,
			)

			// Count the request as it won't be counted by TelemetryInterceptor
			metrics.ServiceRequests.With(metricsHandler).Record(1)

			// Handle the error for metrics and logging
			i.requestErrorHandler.HandleError(
				req,
				info.FullMethod,
				metricsHandler,
				logTags,
				originalErr,
				nsName,
			)

			// Continue with the original error for further processing
			err = originalErr
		}

		// Convert serialization errors to be captured as serviceerrors across gRPC calls
		var deserializationError *serialization.DeserializationError
		var serializationError *serialization.SerializationError
		if errors.As(err, &deserializationError) || errors.As(err, &serializationError) {
			err = serviceerror.NewDataLoss(err.Error())
		}
	}

	return resp, serviceerror.ToStatus(err).Err()
}

package grpcfaults

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a unary server interceptor that checks the generator for faults
// before and after the handler.
//
// This allows tests to inspect requests/responses and inject faults on demand.
//
// Behavior:
//   - If no generator is configured, no interceptor is installed.
//   - The generator is checked before the handler. A fault error skips the handler; a fault response
//     is returned without running it.
//   - The generator is checked after the handler with its response and error. A fault error replaces
//     them; a fault response replaces them without an error.
//   - If no fault matches, the handler's response and error are returned.
func UnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Check before the handler. A fault can short-circuit the call.
		faultResp, faultErr := generator.GenerateRequest(ctx, info.FullMethod, req)
		if faultErr != nil {
			return nil, faultErr
		}
		if faultResp != nil {
			return faultResp, nil
		}

		// Call the handler.
		resp, err := handler(ctx, req)

		// Check after the handler. A fault can replace the response or error.
		faultResp, faultErr = generator.GenerateResponse(ctx, info.FullMethod, req, resp, err)
		if faultErr != nil {
			return nil, faultErr
		}
		if faultResp != nil {
			return faultResp, nil
		}
		return resp, err
	}
}

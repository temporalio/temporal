package services

import (
	"context"

	"google.golang.org/grpc"
)

func registerServiceOverlay(
	registrar grpc.ServiceRegistrar,
	stableDesc grpc.ServiceDesc,
	stable any,
	overlayDesc grpc.ServiceDesc,
	overlay any,
) {
	desc := stableDesc
	stableMethods := make(map[string]struct{}, len(desc.Methods))
	for _, method := range desc.Methods {
		stableMethods[method.MethodName] = struct{}{}
	}

	desc.Methods = append([]grpc.MethodDesc{}, desc.Methods...)
	for _, method := range overlayDesc.Methods {
		if _, ok := stableMethods[method.MethodName]; ok {
			continue
		}
		handler := method.Handler
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: method.MethodName,
			Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				return handler(overlay, ctx, dec, interceptor)
			},
		})
	}

	stableStreams := make(map[string]struct{}, len(desc.Streams))
	for _, stream := range desc.Streams {
		stableStreams[stream.StreamName] = struct{}{}
	}

	desc.Streams = append([]grpc.StreamDesc{}, desc.Streams...)
	for _, stream := range overlayDesc.Streams {
		if _, ok := stableStreams[stream.StreamName]; ok {
			continue
		}
		handler := stream.Handler
		desc.Streams = append(desc.Streams, grpc.StreamDesc{
			StreamName:    stream.StreamName,
			Handler:       func(_ any, serverStream grpc.ServerStream) error { return handler(overlay, serverStream) },
			ServerStreams: stream.ServerStreams,
			ClientStreams: stream.ClientStreams,
		})
	}

	registrar.RegisterService(&desc, stable)
}

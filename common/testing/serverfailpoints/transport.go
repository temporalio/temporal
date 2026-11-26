package serverfailpoints

import (
	"strings"

	"go.temporal.io/testx/failpoint"
)

// The keys of the failpoints at a transport boundary. They are named after the operation, a gRPC
// method or an HTTP request, so rules and tests can name any operation without declaring a key.
var (
	grpcRequestKeys  = failpoint.ReserveKeyPrefix[error]("grpc:")
	grpcResponseKeys = failpoint.ReserveKeyPrefix[error]("grpc-response:")
	httpRequestKeys  = failpoint.ReserveKeyPrefix[error]("http:")
	httpResponseKeys = failpoint.ReserveKeyPrefix[error]("http-response:")
)

// Keys of the failpoints that fire for every operation of a transport. Their prefixes differ from
// those of the operation keys, so the pattern "grpc:*" does not match them.
var (
	// AnyGRPCKey fires before the handler of every gRPC method runs.
	AnyGRPCKey = failpoint.NewKey[error]("grpc-any:request")
	// AnyGRPCResponseKey fires after the handler of every gRPC method returns.
	AnyGRPCResponseKey = failpoint.NewKey[error]("grpc-any:response")
	// AnyHTTPKey fires before every outgoing HTTP request is sent.
	AnyHTTPKey = failpoint.NewKey[error]("http-any:request")
	// AnyHTTPResponseKey fires after every outgoing HTTP request returns.
	AnyHTTPResponseKey = failpoint.NewKey[error]("http-any:response")
)

// GRPCKey returns the key of the failpoint that fires before the handler of a gRPC method runs.
// fullMethod is the full method name, for example "/pkg.Service/Method".
func GRPCKey(fullMethod string) failpoint.Key[error] {
	return grpcRequestKeys.Key(fullMethod)
}

// GRPCResponseKey returns the key of the failpoint that fires after the handler of a gRPC method
// returns.
func GRPCResponseKey(fullMethod string) failpoint.Key[error] {
	return grpcResponseKeys.Key(fullMethod)
}

// HTTPKey returns the key of the failpoint that fires before an outgoing HTTP request is sent.
// operation is the request method and path, for example "POST /callback".
func HTTPKey(operation string) failpoint.Key[error] {
	return httpRequestKeys.Key(operation)
}

// HTTPResponseKey returns the key of the failpoint that fires after an outgoing HTTP request
// returns.
func HTTPResponseKey(operation string) failpoint.Key[error] {
	return httpResponseKeys.Key(operation)
}

// GRPCMethod returns the full method name of a gRPC key name, as GRPCKey and GRPCResponseKey build
// it, and false if the name is not one of them.
func GRPCMethod(keyName string) (string, bool) {
	for _, key := range []failpoint.Key[error]{GRPCKey(""), GRPCResponseKey("")} {
		if method, ok := strings.CutPrefix(keyName, key.Name()); ok {
			return method, true
		}
	}
	return "", false
}

// IsTransportKey reports whether keyName is the name of a failpoint at a transport boundary: the
// key of a gRPC method or HTTP operation (before or after its handler), or the key of any
// operation. Only their executions are scoped to the namespace of the request by name or ID.
func IsTransportKey(keyName string) bool {
	switch keyName {
	case AnyGRPCKey.Name(), AnyGRPCResponseKey.Name(), AnyHTTPKey.Name(), AnyHTTPResponseKey.Name():
		return true
	}
	for _, key := range []failpoint.Key[error]{GRPCKey(""), GRPCResponseKey(""), HTTPKey(""), HTTPResponseKey("")} {
		if strings.HasPrefix(keyName, key.Name()) {
			return true
		}
	}
	return false
}

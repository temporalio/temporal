package headers

import (
	"context"
	"net/http"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/grpc/metadata"
)

// Note the nexusoperations component references these headers and adds them to a list of disallowed headers for users to set.
// If any other headers are added for internal use, they should be added to the disallowed headers list.
const (
	ClientNameHeaderName              = "client-name"
	ClientVersionHeaderName           = "client-version"
	SupportedServerVersionsHeaderName = "supported-server-versions"
	SupportedFeaturesHeaderName       = "supported-features"
	SupportedFeaturesHeaderDelim      = ","

	CallerNameHeaderName = "caller-name"
	CallerTypeHeaderName = "caller-type"
	CallOriginHeaderName = "call-initiation"

	// Principal of the immediate caller (the worker / SDK client that issued
	// this RPC). Set server-side by the auth interceptor; stripped on ingress
	// to prevent spoofing by external callers.
	PrincipalTypeHeaderName = "temporal-principal-type"
	PrincipalNameHeaderName = "temporal-principal-name"
	// Display-name snapshot for the immediate caller when its Name is an opaque
	// ID (Cloud). Captured at auth time; empty when Name is already readable.
	PrincipalResolvedNameHeaderName = "temporal-principal-resolved-name"

	// End-user principal: the identity that originated the request at the edge
	// (e.g. who started the root workflow). Propagated frontend->history (see
	// propagateHeaders) so it reaches NewWorkflow's RootCallerPrincipal seeding.
	EndUserPrincipalTypeHeaderName = "temporal-end-user-principal-type"
	EndUserPrincipalNameHeaderName = "temporal-end-user-principal-name"

	ExperimentHeaderName = "temporal-experiment"
)

var (
	// propagateHeaders are the headers to propagate from the frontend to
	// other services via gRPC metadata. The end-user principal pair is
	// included so the chain-originating identity reaches history's
	// RootCallerPrincipal seeding (see EndUserPrincipalTypeHeaderName).
	propagateHeaders = []string{
		ClientNameHeaderName,
		ClientVersionHeaderName,
		SupportedServerVersionsHeaderName,
		SupportedFeaturesHeaderName,
		CallerNameHeaderName,
		CallerTypeHeaderName,
		CallOriginHeaderName,
		PrincipalTypeHeaderName,
		PrincipalNameHeaderName,
		PrincipalResolvedNameHeaderName,
		EndUserPrincipalTypeHeaderName,
		EndUserPrincipalNameHeaderName,
	}

	// principalHeaderNames is the set of headers that must be stripped from
	// inbound metadata / HTTP requests to prevent external callers from
	// spoofing identity. Includes both immediate-caller and end-user pairs
	// even though the end-user pair is not in propagateHeaders: it can
	// arrive on the Nexus dispatch HTTP boundary, and any external attempt
	// to inject it at a gRPC ingress must still be removed.
	principalHeaderNames = []string{
		PrincipalTypeHeaderName,
		PrincipalNameHeaderName,
		PrincipalResolvedNameHeaderName,
		EndUserPrincipalTypeHeaderName,
		EndUserPrincipalNameHeaderName,
	}
)

// GetValues returns header values for passed header names.
// It always returns slice of the same size as number of passed header names.
func GetValues(ctx context.Context, headerNames ...string) []string {
	headerValues := make([]string, len(headerNames))

	for i, headerName := range headerNames {
		if values := metadata.ValueFromIncomingContext(ctx, headerName); len(values) > 0 {
			headerValues[i] = values[0]
		}
	}

	return headerValues
}

// Propagate propagates version headers from incoming context to outgoing context.
// It copies all headers to outgoing context only if they are exist in incoming context
// and doesn't exist in outgoing context already.
func Propagate(ctx context.Context) context.Context {
	headersToAppend := make([]string, 0, len(propagateHeaders)*2)
	mdOutgoing, mdOutgoingExist := metadata.FromOutgoingContext(ctx)
	for _, headerName := range propagateHeaders {
		if incomingValue := metadata.ValueFromIncomingContext(ctx, headerName); len(incomingValue) > 0 && len(mdOutgoing.Get(headerName)) == 0 {
			headersToAppend = append(headersToAppend, headerName, incomingValue[0])
		}
	}
	if headersToAppend != nil {
		if mdOutgoingExist {
			ctx = metadata.AppendToOutgoingContext(ctx, headersToAppend...)
		} else {
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(headersToAppend...))
		}
	}
	return ctx
}

// HeaderGetter is an interface for getting a single header value from a case insensitive key.
type HeaderGetter interface {
	Get(string) string
}

// Wrapper for gRPC metadata that exposes a helper to extract a single metadata value.
type GRPCHeaderGetter struct {
	ctx context.Context
}

func NewGRPCHeaderGetter(ctx context.Context) GRPCHeaderGetter {
	return GRPCHeaderGetter{ctx: ctx}
}

// Get a single value from the underlying gRPC metadata.
// Returns an empty string if the metadata key is unset.
func (h GRPCHeaderGetter) Get(key string) string {
	if values := metadata.ValueFromIncomingContext(h.ctx, key); len(values) > 0 {
		return values[0]
	}
	return ""
}

// IsExperimentRequested checks if a specific experiment is present in the temporal-experiment header.
// Returns true if the experiment is explicitly listed or if "*" (wildcard) is present.
// Headers exceeding a length of 100 will be skipped.
func IsExperimentRequested(ctx context.Context, experiment string) bool {
	experimentalValues := metadata.ValueFromIncomingContext(ctx, ExperimentHeaderName)

	for _, headerValue := range experimentalValues {
		// limit value size to prevent misuse
		if len(headerValue) > 100 {
			continue
		}
		for requested := range strings.SplitSeq(headerValue, ",") {
			requested = strings.TrimSpace(requested)
			if requested == "*" || requested == experiment {
				return true
			}
		}
	}

	return false
}

// StripPrincipalHTTP removes both the immediate-caller and end-user principal
// HTTP headers from an inbound request. This complements StripPrincipal at
// HTTP ingress boundaries (e.g. the Nexus dispatch and completion HTTP
// handlers) where the request never passes through the gRPC interceptor
// chain, so principal-bearing HTTP headers would otherwise survive into the
// authorized context.
func StripPrincipalHTTP(h http.Header) {
	for _, name := range principalHeaderNames {
		h.Del(name)
	}
}

// StripPrincipal removes both the immediate-caller and end-user principal
// headers from incoming metadata to prevent external callers from spoofing
// identity. Callers must invoke this on every external ingress boundary
// (gRPC frontend interceptor, Nexus dispatch HTTP handler, Nexus completion
// HTTP handler) before authorizing the request.
func StripPrincipal(ctx context.Context) context.Context {
	mdIncoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	for _, h := range principalHeaderNames {
		mdIncoming.Delete(h)
	}
	return metadata.NewIncomingContext(ctx, mdIncoming)
}

// SetPrincipal sets the immediate-caller principal headers in the incoming
// metadata.
func SetPrincipal(ctx context.Context, principal *commonpb.Principal) context.Context {
	return setIncomingMD(ctx, map[string]string{
		PrincipalTypeHeaderName: principal.GetType(),
		PrincipalNameHeaderName: principal.GetName(),
	})
}

// SetPrincipalResolvedName sets the immediate-caller display-name snapshot.
func SetPrincipalResolvedName(ctx context.Context, name string) context.Context {
	return setIncomingMD(ctx, map[string]string{PrincipalResolvedNameHeaderName: name})
}

// GetPrincipalResolvedName retrieves the immediate-caller display-name snapshot,
// or "" if absent (Name is already human-readable).
func GetPrincipalResolvedName(ctx context.Context) string {
	return GetValues(ctx, PrincipalResolvedNameHeaderName)[0]
}

// GetPrincipal retrieves the immediate-caller principal from the context
// headers. Returns nil if no principal-carrying header is present (e.g. the
// caller-side did not opt into propagation, or the request crossed a worker
// boundary so the chain broke).
func GetPrincipal(ctx context.Context) *commonpb.Principal {
	values := GetValues(ctx, PrincipalTypeHeaderName, PrincipalNameHeaderName)
	if values[0] == "" && values[1] == "" {
		return nil
	}
	return &commonpb.Principal{Type: values[0], Name: values[1]}
}

// SetEndUserPrincipal sets the end-user principal headers in the incoming
// metadata. The end-user principal identifies the original initiator of the
// request chain (e.g. the API-key holder who started the root workflow),
// distinct from the immediate caller whose RPC is currently being processed.
func SetEndUserPrincipal(ctx context.Context, principal *commonpb.Principal) context.Context {
	return setIncomingMD(ctx, map[string]string{
		EndUserPrincipalTypeHeaderName: principal.GetType(),
		EndUserPrincipalNameHeaderName: principal.GetName(),
	})
}

// GetEndUserPrincipal retrieves the end-user principal from the context
// headers. Returns nil if no end-user principal-carrying header is present.
func GetEndUserPrincipal(ctx context.Context) *commonpb.Principal {
	values := GetValues(ctx, EndUserPrincipalTypeHeaderName, EndUserPrincipalNameHeaderName)
	if values[0] == "" && values[1] == "" {
		return nil
	}
	return &commonpb.Principal{Type: values[0], Name: values[1]}
}

// setIncomingMD sets the key-value pairs in the incoming metadata.
// Empty values are ignored.
func setIncomingMD(ctx context.Context, kv map[string]string) context.Context {
	mdIncoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		mdIncoming = metadata.MD{}
	}
	for k, v := range kv {
		if v != "" {
			mdIncoming.Set(k, v)
		}
	}
	return metadata.NewIncomingContext(ctx, mdIncoming)
}

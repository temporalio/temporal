package faultinjection

import "net/http"

// HTTPFaultRequest is passed as the `req` argument to the fault generator for HTTP calls,
// so a callback can namespace-scope a fault via GetNamespaceId (matching the gRPC
// convention, where the request proto exposes the namespace) and, if it needs to, inspect
// the underlying request.
type HTTPFaultRequest struct {
	NamespaceID string
	Request     *http.Request
}

// GetNamespaceId lets namespace-scoped fault callbacks match HTTP calls the same way they
// match gRPC requests.
func (r *HTTPFaultRequest) GetNamespaceId() string { return r.NamespaceID }

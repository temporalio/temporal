package httpfailpoints

import (
	"net/http"
	"strings"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator returns a generator that executes the HTTP failpoints of source, scoped to the
// namespace ID of the scope passed to httpfaults.Wrap. namespaces resolves the name of the namespace
// for the scope of BUGGIFY; its fields can be nil. It returns nil if failpoints are not enabled, so no
// HTTP faults are injected.
func NewGenerator(source *failpoint.Registry, namespaces failpointadapter.Namespaces) httpfaults.Generator {
	if source == nil {
		return nil
	}
	return failpointadapter.NewAdapter[*httpfaults.Request, *http.Response](source, failpointadapter.Keys{
		Request:     serverfailpoints.HTTPKey,
		Response:    serverfailpoints.HTTPResponseKey,
		AnyRequest:  serverfailpoints.AnyHTTPKey,
		AnyResponse: serverfailpoints.AnyHTTPResponseKey,
		// The operation is the request method and path. The path contains IDs, so the Policy of
		// the registry, such as BUGGIFY, decides per request method, which keeps the activation of
		// the same seed stable between runs and the number of names bounded. Registered failpoints
		// still match the method and path.
		PolicyOperation: func(operation string) string {
			method, _, _ := strings.Cut(operation, " ")
			return method
		},
	}, namespaces)
}

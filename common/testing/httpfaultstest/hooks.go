package httpfaultstest

import (
	"net/http"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator returns a generator that executes the HTTP failpoints of source, scoped to the
// namespace ID of the scope passed to httpfaults.Wrap. It returns nil if failpoints are not enabled, so no
// HTTP faults are injected.
func NewGenerator(source *failpoints.Source) httpfaults.Generator {
	if !source.Enabled() {
		return nil
	}
	return faultinjectiontest.NewAdapter[*httpfaults.Request, *http.Response](source, faultinjectiontest.Keys{
		Request:     failpoint.HTTPKey,
		Response:    failpoint.HTTPResponseKey,
		AnyRequest:  failpoint.AnyHTTPKey,
		AnyResponse: failpoint.AnyHTTPResponseKey,
	}, nil)
}

package httpfaultstest

import (
	"net/http"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator returns a generator that executes the HTTP failpoints of test hooks, scoped to the
// namespace ID of the scope passed to httpfaults.Wrap. It returns nil without test hooks, so no
// HTTP faults are injected.
func NewGenerator(testHooks commontesthooks.TestHooks) httpfaults.Generator {
	if !testHooks.Enabled() {
		return nil
	}
	return faultinjectiontest.NewAdapter[*httpfaults.Request, *http.Response](testHooks, faultinjectiontest.Keys{
		Request:     failpoint.HTTPKey,
		Response:    failpoint.HTTPResponseKey,
		AnyRequest:  failpoint.AnyHTTPKey,
		AnyResponse: failpoint.AnyHTTPResponseKey,
	}, nil)
}

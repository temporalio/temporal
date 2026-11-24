package nexus

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrRedirectNotAllowed is returned by HTTP clients making outbound Nexus requests (operation start and cancel, and
// completion callbacks) when the destination responds with a redirect.
//
// The Nexus HTTP protocol has no use for redirects. Following them is also unsafe: for 301, 302 and 303 Go's
// http.Client rewrites the POST into a GET without a body, so a redirect would point the server at targets that only
// answer GET requests (such as cloud instance-metadata services) rather than at a Nexus handler. Callers should treat
// this error as non-retryable, since the destination will keep redirecting.
var ErrRedirectNotAllowed = errors.New("destination responded with an HTTP redirect, which is not supported for Nexus requests")

// RejectRedirects wraps rt so that any 3xx response fails the request with [ErrRedirectNotAllowed] instead of being
// returned or followed.
//
// This is done in the transport rather than with http.Client.CheckRedirect because CheckRedirect only sees the
// redirects the client would follow: a 307 or 308 for a request without GetBody is returned as a plain response, and
// would be retried as an unexpected status. Rejecting every 3xx here gives all redirects the same, non-retryable
// handling. http.Client.Do wraps the error in a *url.Error, so match it with errors.Is.
func RejectRedirects(rt http.RoundTripper) http.RoundTripper {
	return redirectRejectingTransport{rt: rt}
}

type redirectRejectingTransport struct {
	rt http.RoundTripper
}

func (t redirectRejectingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.rt.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("%w: %s", ErrRedirectNotAllowed, response.Status)
	}
	return response, nil
}

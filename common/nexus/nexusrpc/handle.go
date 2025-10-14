package nexusrpc

import (
	"context"
	"net/http"
	"net/url"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// An OperationHandle is used to cancel operations and get their result and status.
type OperationHandle[T any] struct {
	// Name of the Operation this handle represents.
	Operation string
	// Handler generated token for this handle's operation.
	Token string

	client *HTTPClient
}

// Cancel requests to cancel an asynchronous operation.
//
// Cancelation is asynchronous and may be not be respected by the operation's implementation.
func (h *OperationHandle[T]) Cancel(ctx context.Context, options nexus.CancelOperationOptions) error {
	u := h.client.serviceBaseURL.JoinPath(url.PathEscape(h.client.options.Service), url.PathEscape(h.Operation), "cancel")
	request, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set(nexus.HeaderOperationToken, h.Token)
	addContextTimeoutToHTTPHeader(ctx, request.Header)
	request.Header.Set(headerUserAgent, userAgent)
	addNexusHeaderToHTTPHeader(options.Header, request.Header)
	response, err := h.client.options.HTTPCaller(request)
	if err != nil {
		return err
	}

	// Do this once here and make sure it doesn't leak.
	body, err := readAndReplaceBody(response)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusAccepted {
		return h.client.bestEffortHandlerErrorFromResponse(response, body)
	}
	return nil
}

package nexusoperations

import (
	"errors"
	"io"
	"net/http"

	"go.temporal.io/server/common/rpc"
)

var ErrResponseBodyTooLarge = errors.New("http: response body too large")

// A LimitedReaderCloser reads from R but limits the amount of data returned to just N bytes. Each call to Read updates
// N to reflect the new amount remaining. Read returns [ErrResponseBodyTooLarge] when N <= 0.
type LimitedReadCloser struct {
	R io.ReadCloser
	N int64
}

func NewLimitedReadCloser(rc io.ReadCloser, l int64) *LimitedReadCloser {
	return &LimitedReadCloser{rc, l}
}

func (l *LimitedReadCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, ErrResponseBodyTooLarge
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}

func (l *LimitedReadCloser) Close() error {
	return l.R.Close()
}

type ResponseSizeLimiter struct {
	roundTripper http.RoundTripper
}

func (r ResponseSizeLimiter) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.roundTripper.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	// Limit the response body to max allowed Payload size.
	// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate limit
	// should be enforced on top of this when generating the NexusOperationCompleted history event.
	response.Body = NewLimitedReadCloser(response.Body, rpc.MaxNexusAPIRequestBodyBytes)
	return response, nil
}

var _ http.RoundTripper = ResponseSizeLimiter{}

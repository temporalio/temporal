// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package replication

import (
	"context"
	"fmt"
	"io"
	"sync"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

const (
	streamStatusInitialized int32 = 0
	streamStatusOpen        int32 = 1
	streamStatusClosed      int32 = 2
)

const (
	defaultChanSize = 512 // make the buffer size large enough so buffer will not be blocked
)

var (
	// ErrClosed indicates stream closed before a read/write operation
	ErrClosed = serviceerror.NewUnavailable("stream closed")
)

type (
	BiDirectionStreamClientProvider[Req any, Resp any] interface {
		Get(ctx context.Context) (BiDirectionStreamClient[Req, Resp], error)
	}
	BiDirectionStreamClient[Req any, Resp any] interface {
		Send(Req) error
		Recv() (Resp, error)
		CloseSend() error
	}
	BiDirectionStream[Req any, Resp any] interface {
		Send(Req) error
		Recv() (<-chan StreamResp[Resp], error)
		Close()
		IsValid() bool
	}
	StreamResp[Resp any] struct {
		Resp Resp
		Err  error
	}
	BiDirectionStreamImpl[Req any, Resp any] struct {
		ctx            context.Context
		clientProvider BiDirectionStreamClientProvider[Req, Resp]
		metricsHandler metrics.Handler
		logger         log.Logger

		sync.Mutex
		status          int32
		channel         chan StreamResp[Resp]
		streamingClient BiDirectionStreamClient[Req, Resp]
	}

	StreamError struct {
		Message string
		cause   error
	}
)

func NewBiDirectionStream[Req any, Resp any](
	clientProvider BiDirectionStreamClientProvider[Req, Resp],
	metricsHandler metrics.Handler,
	logger log.Logger,
) *BiDirectionStreamImpl[Req, Resp] {
	return &BiDirectionStreamImpl[Req, Resp]{
		ctx:            context.Background(),
		clientProvider: clientProvider,
		metricsHandler: metricsHandler,
		logger:         logger,

		status:          streamStatusInitialized,
		channel:         make(chan StreamResp[Resp], defaultChanSize),
		streamingClient: nil,
	}
}

func (s *BiDirectionStreamImpl[Req, Resp]) Send(
	request Req,
) error {
	s.Lock()
	defer s.Unlock()

	if err := s.lazyInitLocked(); err != nil {
		return NewStreamError("BiDirectionStream send initialize error", err)
	}
	if err := s.streamingClient.Send(request); err != nil {
		s.closeLocked()
		return NewStreamError("BiDirectionStream send error", err)
	}
	return nil
}

func (s *BiDirectionStreamImpl[Req, Resp]) Recv() (<-chan StreamResp[Resp], error) {
	s.Lock()
	defer s.Unlock()

	if err := s.lazyInitLocked(); err != nil {
		return nil, NewStreamError("BiDirectionStream recv initialize error", err)
	}
	return s.channel, nil

}

func (s *BiDirectionStreamImpl[Req, Resp]) Close() {
	s.Lock()
	defer s.Unlock()

	s.closeLocked()
}

func (s *BiDirectionStreamImpl[Req, Resp]) IsValid() bool {
	s.Lock()
	defer s.Unlock()
	return s.status != streamStatusClosed
}

func (s *BiDirectionStreamImpl[Req, Resp]) closeLocked() {
	if s.status == streamStatusClosed {
		return
	}
	s.status = streamStatusClosed
	if s.streamingClient != nil {
		err := s.streamingClient.CloseSend() // if there is error, the stream is also closed
		if err != nil {
			s.logger.Error("BiDirectionStream close error", tag.Error(err))
		}
	}
}

func (s *BiDirectionStreamImpl[Req, Resp]) lazyInitLocked() error {
	switch s.status {
	case streamStatusInitialized:
		streamingClient, err := s.clientProvider.Get(s.ctx)
		if err != nil {
			return err
		}
		s.streamingClient = streamingClient
		s.status = streamStatusOpen
		go s.recvLoop()
		return nil
	case streamStatusOpen:
		return nil
	case streamStatusClosed:
		return ErrClosed
	default:
		panic(fmt.Sprintf("upload stream unknown status: %v", s.status))
	}
}

func (s *BiDirectionStreamImpl[Req, Resp]) recvLoop() {
	defer close(s.channel)
	defer s.Close()

	for {
		resp, err := s.streamingClient.Recv()
		switch err {
		case nil:
			s.channel <- StreamResp[Resp]{
				Resp: resp,
				Err:  nil,
			}
		case io.EOF:
			return
		default:
			s.logger.Error(fmt.Sprintf(
				"BiDirectionStream encountered unexpected error, closing: %T %s",
				err, err,
			))
			var errResp Resp
			s.channel <- StreamResp[Resp]{
				Resp: errResp,
				Err:  NewStreamError("BiDirectionStream recv error", err),
			}
			return
		}
	}
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("StreamError: %s | GRPC Error: %v", e.Message, e.cause)
}

func NewStreamError(message string, err error) *StreamError {
	return &StreamError{
		Message: message,
		cause:   err,
	}
}

package replication

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	streamStatusInitialized int32 = 0
	streamStatusOpen        int32 = 1
	streamStatusClosed      int32 = 2
	recvPausePollInterval         = 100 * time.Millisecond
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
		ctx             context.Context
		cancel          context.CancelFunc
		clientProvider  BiDirectionStreamClientProvider[Req, Resp]
		metricsHandler  metrics.Handler
		logger          log.Logger
		shouldPauseRecv func() bool

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
	shouldPauseRecv func() bool,
) *BiDirectionStreamImpl[Req, Resp] {
	ctx, cancel := context.WithCancel(context.Background())
	return &BiDirectionStreamImpl[Req, Resp]{
		ctx:             ctx,
		cancel:          cancel,
		clientProvider:  clientProvider,
		metricsHandler:  metricsHandler,
		logger:          logger,
		shouldPauseRecv: shouldPauseRecv,

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
	s.cancel()
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
		if !s.waitUntilRecvResumed() {
			return
		}
		resp, err := s.streamingClient.Recv()
		switch err {
		case nil:
			s.notifyRecvChannel(resp, nil)
		case io.EOF:
			return
		default:
			var errResp Resp
			s.notifyRecvChannel(errResp, NewStreamError("BiDirectionStream recv error", err))
			return
		}
	}
}

func (s *BiDirectionStreamImpl[Req, Resp]) waitUntilRecvResumed() bool {
	if s.shouldPauseRecv == nil || !s.shouldPauseRecv() {
		return true
	}

	s.logger.Info("Replication stream receive paused")
	ticker := time.NewTicker(recvPausePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return false
		case <-ticker.C:
			if !s.shouldPauseRecv() {
				s.logger.Info("Replication stream receive resumed")
				return true
			}
		}
	}
}

func (s *BiDirectionStreamImpl[Req, Resp]) notifyRecvChannel(response Resp, err error) {
	resp := StreamResp[Resp]{
		Resp: response,
		Err:  err,
	}

	select {
	case s.channel <- resp:
		return
	default:
		metrics.ReplicationStreamChannelFull.With(s.metricsHandler).Record(1)
		s.channel <- resp
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

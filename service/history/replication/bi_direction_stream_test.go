package replication

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

type (
	biDirectionStreamSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		biDirectionStream    *BiDirectionStreamImpl[int, int]
		streamClientProvider *mockStreamClientProvider
		streamClient         *mockStreamClient
		streamErrClient      *mockStreamErrClient
	}

	mockStreamClientProvider struct {
		streamClient BiDirectionStreamClient[int, int]
	}
	mockStreamClient struct {
		shutdownChan chan struct{}

		requests []int

		responseCount int
		responses     []int
	}
	mockStreamErrClient struct {
		sendErr error
		recvErr error
	}
)

func TestBiDirectionStreamSuite(t *testing.T) {
	s := new(biDirectionStreamSuite)
	suite.Run(t, s)
}

func (s *biDirectionStreamSuite) SetupSuite() {

}

func (s *biDirectionStreamSuite) TearDownSuite() {

}

func (s *biDirectionStreamSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.streamClient = &mockStreamClient{
		shutdownChan:  make(chan struct{}),
		requests:      nil,
		responseCount: 10,
		responses:     nil,
	}
	s.streamErrClient = &mockStreamErrClient{
		sendErr: serviceerror.NewUnavailable("random send error"),
		recvErr: serviceerror.NewUnavailable("random recv error"),
	}
	s.streamClientProvider = &mockStreamClientProvider{streamClient: s.streamClient}
	s.biDirectionStream = NewBiDirectionStream[int, int](
		s.streamClientProvider,
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
	)
}

func (s *biDirectionStreamSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *biDirectionStreamSuite) TestLazyInit() {
	s.Nil(s.biDirectionStream.streamingClient)

	s.biDirectionStream.Lock()
	err := s.biDirectionStream.lazyInitLocked()
	s.biDirectionStream.Unlock()
	s.NoError(err)
	s.Equal(s.streamClient, s.biDirectionStream.streamingClient)
	s.True(s.biDirectionStream.IsValid())

	s.biDirectionStream.Lock()
	err = s.biDirectionStream.lazyInitLocked()
	s.biDirectionStream.Unlock()
	s.NoError(err)
	s.Equal(s.streamClient, s.biDirectionStream.streamingClient)
	s.True(s.biDirectionStream.IsValid())

	s.biDirectionStream.Close()
	s.biDirectionStream.Lock()
	err = s.biDirectionStream.lazyInitLocked()
	s.biDirectionStream.Unlock()
	s.Error(err)
	s.False(s.biDirectionStream.IsValid())
}

func (s *biDirectionStreamSuite) TestSend() {
	defer close(s.streamClient.shutdownChan)

	reqs := []int{rand.Int(), rand.Int(), rand.Int(), rand.Int()}
	for _, req := range reqs {
		err := s.biDirectionStream.Send(req)
		s.NoError(err)
	}
	s.Equal(reqs, s.streamClient.requests)
	s.True(s.biDirectionStream.IsValid())
}

func (s *biDirectionStreamSuite) TestSend_Err() {
	defer close(s.streamClient.shutdownChan)

	s.streamClientProvider.streamClient = s.streamErrClient

	err := s.biDirectionStream.Send(rand.Int())
	s.Error(err)
	s.False(s.biDirectionStream.IsValid())
}

func (s *biDirectionStreamSuite) TestRecv() {
	close(s.streamClient.shutdownChan)

	var resps []int
	streamRespChan, err := s.biDirectionStream.Recv()
	s.NoError(err)
	for streamResp := range streamRespChan {
		s.NoError(streamResp.Err)
		resps = append(resps, streamResp.Resp)
	}
	s.Equal(s.streamClient.responses, resps)
	s.False(s.biDirectionStream.IsValid())
}

func (s *biDirectionStreamSuite) TestRecv_Err() {
	close(s.streamClient.shutdownChan)
	s.streamClientProvider.streamClient = s.streamErrClient

	streamRespChan, err := s.biDirectionStream.Recv()
	s.NoError(err)
	streamResp := <-streamRespChan
	s.Error(streamResp.Err)
	_, ok := <-streamRespChan
	s.False(ok)
	s.False(s.biDirectionStream.IsValid())

}

func TestBiDirectionStreamCloseFullReceiveChannel(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	full := make(chan struct{})
	handler := metrics.NewMockHandler(controller)
	handler.EXPECT().Counter("replication_stream_channel_full").Return(metrics.CounterFunc(func(int64, ...metrics.Tag) {
		close(full)
	}))
	client := &mockStreamClient{shutdownChan: make(chan struct{}), responseCount: 1}
	stream := NewBiDirectionStream[int, int](&mockStreamClientProvider{streamClient: client}, handler, log.NewTestLogger())
	stream.streamingClient = client
	stream.status = streamStatusOpen
	for i := 0; i < cap(stream.channel); i++ {
		stream.channel <- StreamResp[int]{Resp: i}
	}
	done := make(chan struct{})
	go func() {
		stream.recvLoop()
		close(done)
	}()
	t.Cleanup(func() {
		close(client.shutdownChan)
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case <-done:
				return
			case <-stream.channel:
			case <-deadline.C:
				t.Error("receive loop did not exit during cleanup")
				return
			}
		}
	})
	select {
	case <-full:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not reach the full-channel path")
	}
	stream.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closed transport kept its receive producer blocked on a full channel")
	}
	// Do not drain before the completion assertion: that would rescue the old implementation.
	require.Len(t, stream.channel, defaultChanSize)
	for range stream.channel {
	}
	require.False(t, stream.IsValid())
}

func TestBiDirectionStreamCloseBlockedIO(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"get", "send", "recv"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			entered := make(chan struct{})
			release := make(chan struct{})
			var clientCtx context.Context
			provider := streamClientProviderFunc[int, int](func(ctx context.Context) (BiDirectionStreamClient[int, int], error) {
				clientCtx = ctx
				if operation == "get" {
					close(entered)
					return nil, waitForStreamCancellation(ctx, release)
				}
				return &streamClientStub[int, int]{
					send: func(int) error {
						close(entered)
						return waitForStreamCancellation(ctx, release)
					},
					recv: func() (int, error) {
						if operation == "recv" {
							close(entered)
						}
						return 0, waitForStreamCancellation(ctx, release)
					},
				}, nil
			})
			stream := NewBiDirectionStream[int, int](provider, metrics.NoopMetricsHandler, log.NewTestLogger())
			t.Cleanup(func() {
				close(release)
				stream.Close()
			})
			result := make(chan error, 1)
			if operation == "recv" {
				_, err := stream.Recv()
				require.NoError(t, err)
			} else {
				go func() { result <- stream.Send(1) }()
			}
			waitForStreamSignal(t, entered, "I/O did not start")
			closed := make(chan struct{})
			go func() {
				stream.Close()
				close(closed)
			}()
			waitForStreamSignal(t, clientCtx.Done(), "Close did not cancel the provider context")
			waitForStreamSignal(t, closed, "Close remained blocked behind I/O")
			if operation != "recv" {
				select {
				case err := <-result:
					require.Error(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("Send did not return after Close")
				}
			}
			if operation != "get" {
				select {
				case _, ok := <-stream.channel:
					require.False(t, ok, "intentional close should terminate the receive channel")
				case <-time.After(5 * time.Second):
					t.Fatal("receive channel stayed open after Close")
				}
			}
			stream.Close()
			require.False(t, stream.IsValid())
		})
	}
}

func TestBiDirectionStreamCloseBeforeInitialization(t *testing.T) {
	t.Parallel()

	provider := streamClientProviderFunc[int, int](func(context.Context) (BiDirectionStreamClient[int, int], error) {
		t.Error("closed stream initialized its provider")
		return nil, io.EOF
	})
	stream := NewBiDirectionStream[int, int](provider, metrics.NoopMetricsHandler, log.NewTestLogger())
	stream.Close()
	stream.Close()
	require.False(t, stream.IsValid())
	require.Error(t, stream.Send(1))
	channel, err := stream.Recv()
	require.Error(t, err)
	require.Nil(t, channel)
}

func TestBiDirectionStreamCloseOnSendError(t *testing.T) {
	t.Parallel()

	recvEntered := make(chan struct{})
	release := make(chan struct{})
	var clientCtx context.Context
	provider := streamClientProviderFunc[int, int](func(ctx context.Context) (BiDirectionStreamClient[int, int], error) {
		clientCtx = ctx
		return &streamClientStub[int, int]{
			send: func(int) error {
				select {
				case <-recvEntered:
				case <-release:
				}
				return io.ErrClosedPipe
			},
			recv: func() (int, error) {
				close(recvEntered)
				return 0, waitForStreamCancellation(ctx, release)
			},
		}, nil
	})
	stream := NewBiDirectionStream[int, int](provider, metrics.NoopMetricsHandler, log.NewTestLogger())
	t.Cleanup(func() {
		close(release)
		stream.Close()
	})
	result := make(chan error, 1)
	go func() { result <- stream.Send(1) }()
	waitForStreamSignal(t, recvEntered, "receive did not start before Send failed")
	select {
	case err := <-result:
		require.ErrorContains(t, err, io.ErrClosedPipe.Error())
	case <-time.After(5 * time.Second):
		t.Fatal("Send did not return its error")
	}
	waitForStreamSignal(t, clientCtx.Done(), "Send failure did not cancel the transport")
	select {
	case _, ok := <-stream.channel:
		require.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("Send failure left Recv blocked")
	}
	require.False(t, stream.IsValid())
}

type streamClientProviderFunc[Req any, Resp any] func(context.Context) (BiDirectionStreamClient[Req, Resp], error)

func (p streamClientProviderFunc[Req, Resp]) Get(ctx context.Context) (BiDirectionStreamClient[Req, Resp], error) {
	return p(ctx)
}

type streamClientStub[Req any, Resp any] struct {
	send func(Req) error
	recv func() (Resp, error)
}

func (c *streamClientStub[Req, Resp]) Send(request Req) error { return c.send(request) }
func (c *streamClientStub[Req, Resp]) Recv() (Resp, error)    { return c.recv() }
func (c *streamClientStub[Req, Resp]) CloseSend() error       { return nil }

func waitForStreamCancellation(ctx context.Context, release <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
		return io.EOF
	}
}

func waitForStreamSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}

func (p *mockStreamClientProvider) Get(
	_ context.Context,
) (BiDirectionStreamClient[int, int], error) {
	return p.streamClient, nil
}

func (c *mockStreamClient) Send(req int) error {
	c.requests = append(c.requests, req)
	return nil
}

func (c *mockStreamClient) Recv() (int, error) {
	if len(c.responses) >= c.responseCount {
		<-c.shutdownChan
		return 0, io.EOF
	}

	resp := rand.Int()
	c.responses = append(c.responses, resp)
	return resp, nil
}

func (c *mockStreamClient) CloseSend() error {
	return nil
}

func (c *mockStreamErrClient) Send(_ int) error {
	return c.sendErr
}

func (c *mockStreamErrClient) Recv() (int, error) {
	return 0, c.recvErr
}

func (c *mockStreamErrClient) CloseSend() error {
	return nil
}

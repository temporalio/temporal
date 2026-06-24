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
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
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

func (s *biDirectionStreamSuite) TestStreamError_Error() {
	cause := serviceerror.NewUnavailable("underlying grpc error")
	streamErr := NewStreamError("some message", cause)
	errStr := streamErr.Error()
	s.Contains(errStr, "some message")
	s.Contains(errStr, "underlying grpc error")
}

func (s *biDirectionStreamSuite) TestLazyInit_ProviderError() {
	// Covers lazyInitLocked branch where clientProvider.Get returns an error.
	provider := &biDirProviderErr{getErr: serviceerror.NewUnavailable("provider get error")}
	stream := NewBiDirectionStream[int, int](
		provider,
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
	)

	stream.Lock()
	err := stream.lazyInitLocked()
	stream.Unlock()
	s.Error(err)
	// status remains initialized, stream still considered valid (not closed).
	s.Equal(streamStatusInitialized, stream.status)

	// Send should also surface the init error.
	err = stream.Send(rand.Int())
	s.Error(err)
}

func (s *biDirectionStreamSuite) TestLazyInit_UnknownStatusPanics() {
	// Covers lazyInitLocked default branch (panic on unknown status).
	stream := NewBiDirectionStream[int, int](
		s.streamClientProvider,
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
	)
	stream.status = int32(99)

	stream.Lock()
	defer stream.Unlock()
	s.Panics(func() {
		_ = stream.lazyInitLocked()
	})
}

func (s *biDirectionStreamSuite) TestClose_CloseSendError() {
	// Covers closeLocked branch where streamingClient.CloseSend returns an error.
	closeErrClient := &biDirCloseErrClient{closeSendErr: serviceerror.NewUnavailable("close send error")}
	provider := &biDirProviderErr{streamClient: closeErrClient}
	stream := NewBiDirectionStream[int, int](
		provider,
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
	)

	// initialize so streamingClient is set and a recvLoop is started.
	stream.Lock()
	err := stream.lazyInitLocked()
	stream.Unlock()
	s.NoError(err)

	stream.Close()
	s.False(stream.IsValid())

	// closing again is a noop (status already closed).
	stream.Close()
	s.False(stream.IsValid())
}

func (s *biDirectionStreamSuite) TestNotifyRecvChannel_Full() {
	// Covers notifyRecvChannel default branch where the channel buffer is full and a
	// metric is recorded before the (blocking) send. A capture handler is used to
	// deterministically detect entry into the default branch before draining the channel.
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	stream := NewBiDirectionStream[int, int](
		s.streamClientProvider,
		metricsHandler,
		log.NewTestLogger(),
	)
	// fill the channel buffer completely so the first select case cannot succeed.
	for i := 0; i < cap(stream.channel); i++ {
		stream.channel <- StreamResp[int]{Resp: i}
	}

	// notifyRecvChannel will hit the default branch (channel full); run it in a goroutine.
	done := make(chan struct{})
	go func() {
		stream.notifyRecvChannel(rand.Int(), nil)
		close(done)
	}()

	// wait until the default branch has recorded the channel-full metric, which guarantees
	// the select took the default path before we make room for the blocked send.
	await.RequireTrue(s.T(), func() bool {
		return len(capture.Snapshot()[metrics.ReplicationStreamChannelFull.Name()]) > 0
	}, time.Second*5, time.Millisecond*10)

	<-stream.channel // make room so the blocked send can proceed
	<-done
}

func (p *mockStreamClientProvider) Get(
	_ context.Context,
) (BiDirectionStreamClient[int, int], error) {
	return p.streamClient, nil
}

type (
	biDirProviderErr struct {
		getErr       error
		streamClient BiDirectionStreamClient[int, int]
	}
	biDirCloseErrClient struct {
		closeSendErr error
	}
)

func (p *biDirProviderErr) Get(
	_ context.Context,
) (BiDirectionStreamClient[int, int], error) {
	if p.getErr != nil {
		return nil, p.getErr
	}
	return p.streamClient, nil
}

func (c *biDirCloseErrClient) Send(_ int) error {
	return nil
}

func (c *biDirCloseErrClient) Recv() (int, error) {
	<-make(chan struct{}) // block until closed; never returns a value
	return 0, io.EOF
}

func (c *biDirCloseErrClient) CloseSend() error {
	return c.closeSendErr
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

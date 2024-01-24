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
	"io"
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

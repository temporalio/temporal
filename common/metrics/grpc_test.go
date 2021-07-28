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

package metrics

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	metricspb "go.temporal.io/server/api/metrics/v1"
	"go.temporal.io/server/common/log"
)

type (
	grpcSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
	}
)

func TestGrpcSuite(t *testing.T) {
	s := new(grpcSuite)
	suite.Run(t, s)
}

func (s *grpcSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *grpcSuite) TearDownTest() {}

func (s *grpcSuite) TestMetadataMetricInjection() {
	logger := log.NewMockLogger(s.controller)
	ctx := context.Background()
	ssts := newMockServerTransportStream()
	ctx = grpc.NewContextWithServerTransportStream(ctx, ssts)
	anyMetricName := "any_metric_name"

	smcii := NewServerMetricsContextInjectorInterceptor()
	s.NotNil(smcii)
	res, err := smcii(
		ctx, nil, nil,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			res, err := NewServerMetricsTrailerPropagatorInterceptor(logger)(
				ctx, req, nil,
				func(ctx context.Context, req interface{}) (interface{}, error) {
					cmtpi := NewClientMetricsTrailerPropagatorInterceptor(logger)
					s.NotNil(cmtpi)
					cmtpi(
						ctx, "any_value", nil, nil, nil,
						func(
							ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
							opts ...grpc.CallOption,
						) error {
							trailer := opts[0].(grpc.TrailerCallOption)
							propagationContext := &metricspb.Baggage{CountersInt: make(map[string]int64)}
							propagationContext.CountersInt[anyMetricName] = 1234
							data, err := propagationContext.Marshal()
							if err != nil {
								s.Fail("failed to marshal values")
							}
							*trailer.TrailerAddr = metadata.MD{}
							trailer.TrailerAddr.Append(metricsTrailerKey, string(data))
							return nil
						},
					)
					return 10, nil
				},
			)

			s.Nil(err)
			s.Equal(len(ssts.trailers), 1)
			propagationContextBlobs := ssts.trailers[0].Get(metricsTrailerKey)
			s.NotNil(propagationContextBlobs)
			s.Equal(1, len(propagationContextBlobs))
			baggage := &metricspb.Baggage{}
			err = baggage.Unmarshal(([]byte)(propagationContextBlobs[0]))
			s.Nil(err)
			s.Equal(int64(1234), baggage.CountersInt[anyMetricName])
			return res, err
		},
	)

	s.Nil(err)
	s.Equal(10, res)
	s.Assert()
}

func (s *grpcSuite) TestMetadataMetricInjection_NoMetricPresent() {
	logger := log.NewMockLogger(s.controller)
	ctx := context.Background()
	ssts := newMockServerTransportStream()
	ctx = grpc.NewContextWithServerTransportStream(ctx, ssts)

	smcii := NewServerMetricsContextInjectorInterceptor()
	s.NotNil(smcii)
	res, err := smcii(
		ctx, nil, nil,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			res, err := NewServerMetricsTrailerPropagatorInterceptor(logger)(
				ctx, req, nil,
				func(ctx context.Context, req interface{}) (interface{}, error) {
					cmtpi := NewClientMetricsTrailerPropagatorInterceptor(logger)
					s.NotNil(cmtpi)
					cmtpi(
						ctx, "any_value", nil, nil, nil,
						func(
							ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
							opts ...grpc.CallOption,
						) error {
							trailer := opts[0].(grpc.TrailerCallOption)
							propagationContext := &metricspb.Baggage{}
							data, err := propagationContext.Marshal()
							if err != nil {
								s.Fail("failed to marshal values")
							}
							trailer.TrailerAddr = &metadata.MD{}
							trailer.TrailerAddr.Append(metricsTrailerKey, string(data))
							return nil
						},
					)
					return 10, nil
				},
			)

			s.Nil(err)
			s.Equal(len(ssts.trailers), 1)
			propagationContextBlobs := ssts.trailers[0].Get(metricsTrailerKey)
			s.NotNil(propagationContextBlobs)
			s.Equal(1, len(propagationContextBlobs))
			baggage := &metricspb.Baggage{}
			err = baggage.Unmarshal(([]byte)(propagationContextBlobs[0]))
			s.Nil(err)
			s.Nil(baggage.CountersInt)
			return res, err
		},
	)

	s.Nil(err)
	s.Equal(10, res)
	s.Assert()
}

func (s *grpcSuite) TestContextCounterAdd() {
	ctx := AddMetricsContext(context.Background())

	testCounterName := "test_counter"
	ContextCounterAdd(ctx, testCounterName, 100)
	ContextCounterAdd(ctx, testCounterName, 20)
	ContextCounterAdd(ctx, testCounterName, 3)

	value, ok := ContextCounterGet(ctx, testCounterName)
	s.True(ok)
	s.Equal(int64(123), value)
}

func (s *grpcSuite) TestContextCounterAddNoMetricsContext() {
	testCounterName := "test_counter"
	ContextCounterAdd(context.Background(), testCounterName, 3)
}

func newMockServerTransportStream() *mockServerTransportStream {
	return &mockServerTransportStream{trailers: []*metadata.MD{}}
}

type mockServerTransportStream struct {
	trailers []*metadata.MD
}

func (s *mockServerTransportStream) Method() string {
	return "mockssts"
}
func (s *mockServerTransportStream) SetHeader(md metadata.MD) error {
	return nil
}
func (s *mockServerTransportStream) SendHeader(md metadata.MD) error {
	return nil
}
func (s *mockServerTransportStream) SetTrailer(md metadata.MD) error {
	mdCopy := md.Copy()
	s.trailers = append(s.trailers, &mdCopy)
	return nil
}

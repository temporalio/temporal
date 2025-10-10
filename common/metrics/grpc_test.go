package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	metricsspb "go.temporal.io/server/api/metrics/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/rpctest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	grpcSuite struct {
		suite.Suite
		controller *gomock.Controller
	}
)

func TestGrpcSuite(t *testing.T) {
	s := new(grpcSuite)
	suite.Run(t, s)
}

func (s *grpcSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
}

func (s *grpcSuite) TearDownTest() {}

func (s *grpcSuite) TestMetadataMetricInjection() {
	logger := log.NewMockLogger(s.controller)
	ctx := context.Background()
	ssts := rpctest.NewMockServerTransportStream("/temporal.test/MetadataMetricInjection")
	ctx = grpc.NewContextWithServerTransportStream(ctx, ssts)
	anyMetricName := "any_metric_name"

	smcii := NewServerMetricsContextInjectorInterceptor()
	require.NotNil(s.T(), smcii)
	res, err := smcii(
		ctx, nil, nil,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			res, err := NewServerMetricsTrailerPropagatorInterceptor(logger)(
				ctx, req, nil,
				func(ctx context.Context, req interface{}) (interface{}, error) {
					cmtpi := NewClientMetricsTrailerPropagatorInterceptor(logger)
					require.NotNil(s.T(), cmtpi)
					cmtpi(
						ctx, "any_value", nil, nil, nil,
						func(
							ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
							opts ...grpc.CallOption,
						) error {
							trailer := opts[0].(grpc.TrailerCallOption)
							propagationContext := &metricsspb.Baggage{CountersInt: make(map[string]int64)}
							propagationContext.CountersInt[anyMetricName] = 1234
							data, err := propagationContext.Marshal()
							if err != nil {
								require.Fail(s.T(), "failed to marshal values")
							}
							*trailer.TrailerAddr = metadata.MD{}
							trailer.TrailerAddr.Append(metricsTrailerKey, string(data))
							return nil
						},
					)
					return 10, nil
				},
			)

			require.Nil(s.T(), err)
			trailers := ssts.CapturedTrailers()
			require.Equal(s.T(), 1, len(trailers))
			propagationContextBlobs := trailers[0].Get(metricsTrailerKey)
			require.NotNil(s.T(), propagationContextBlobs)
			require.Equal(s.T(), 1, len(propagationContextBlobs))
			baggage := &metricsspb.Baggage{}
			err = baggage.Unmarshal(([]byte)(propagationContextBlobs[0]))
			require.Nil(s.T(), err)
			require.Equal(s.T(), int64(1234), baggage.CountersInt[anyMetricName])
			return res, err
		},
	)

	require.Nil(s.T(), err)
	require.Equal(s.T(), 10, res)
	s.Assert()
}

func (s *grpcSuite) TestMetadataMetricInjection_NoMetricPresent() {
	logger := log.NewMockLogger(s.controller)
	ctx := context.Background()
	ssts := rpctest.NewMockServerTransportStream("/temporal.test/MetadataMetricInjectionNoMetric")
	ctx = grpc.NewContextWithServerTransportStream(ctx, ssts)

	smcii := NewServerMetricsContextInjectorInterceptor()
	require.NotNil(s.T(), smcii)
	res, err := smcii(
		ctx, nil, nil,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			res, err := NewServerMetricsTrailerPropagatorInterceptor(logger)(
				ctx, req, nil,
				func(ctx context.Context, req interface{}) (interface{}, error) {
					cmtpi := NewClientMetricsTrailerPropagatorInterceptor(logger)
					require.NotNil(s.T(), cmtpi)
					cmtpi(
						ctx, "any_value", nil, nil, nil,
						func(
							ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
							opts ...grpc.CallOption,
						) error {
							trailer := opts[0].(grpc.TrailerCallOption)
							propagationContext := &metricsspb.Baggage{}
							data, err := propagationContext.Marshal()
							if err != nil {
								require.Fail(s.T(), "failed to marshal values")
							}
							trailer.TrailerAddr = &metadata.MD{}
							trailer.TrailerAddr.Append(metricsTrailerKey, string(data))
							return nil
						},
					)
					return 10, nil
				},
			)

			require.Nil(s.T(), err)
			trailers := ssts.CapturedTrailers()
			require.Equal(s.T(), 1, len(trailers))
			propagationContextBlobs := trailers[0].Get(metricsTrailerKey)
			require.NotNil(s.T(), propagationContextBlobs)
			require.Equal(s.T(), 1, len(propagationContextBlobs))
			baggage := &metricsspb.Baggage{}
			err = baggage.Unmarshal(([]byte)(propagationContextBlobs[0]))
			require.Nil(s.T(), err)
			require.Nil(s.T(), baggage.CountersInt)
			return res, err
		},
	)

	require.Nil(s.T(), err)
	require.Equal(s.T(), 10, res)
	s.Assert()
}

func (s *grpcSuite) TestContextCounterAdd() {
	ctx := AddMetricsContext(context.Background())

	testCounterName := "test_counter"
	ContextCounterAdd(ctx, testCounterName, 100)
	ContextCounterAdd(ctx, testCounterName, 20)
	ContextCounterAdd(ctx, testCounterName, 3)

	value, ok := ContextCounterGet(ctx, testCounterName)
	require.True(s.T(), ok)
	require.Equal(s.T(), int64(123), value)
}

func (s *grpcSuite) TestContextCounterAddNoMetricsContext() {
	testCounterName := "test_counter"
	ContextCounterAdd(context.Background(), testCounterName, 3)
}

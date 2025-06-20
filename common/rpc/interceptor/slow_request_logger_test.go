package interceptor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	testThreshold = 10 * time.Millisecond
)

type slowRequestLoggerSuite struct {
	suite.Suite
	controller *gomock.Controller

	logger      *log.MockLogger
	interceptor *interceptor.SlowRequestLoggerInterceptor
}

func TestSlowRequestLoggerInterceptor(t *testing.T) {
	suite.Run(t, &slowRequestLoggerSuite{})
}

func (s *slowRequestLoggerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewMockLogger(s.controller)
	s.interceptor = interceptor.NewSlowRequestLoggerInterceptor(
		s.logger,
		dynamicconfig.GetDurationPropertyFn(testThreshold),
	)
}

func (s *slowRequestLoggerSuite) TestIntercept() {
	ctx := context.Background()

	// Factory function to return a UnaryHandler that sleeps for a duration.
	makeHandler := func(delay time.Duration) grpc.UnaryHandler {
		return func(_ context.Context, _ any) (any, error) {
			//nolint:forbidigo // Allow time.Sleep for timeout tests
			time.Sleep(delay)
			return nil, nil
		}
	}
	fastHandler := makeHandler(0) // sleep will return immediately
	slowHandler := makeHandler(testThreshold + 1)

	// Dummy request to test extraction.
	request := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: "namespace-name",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-id",
			RunId:      "run-id",
		},
	}
	info := &grpc.UnaryServerInfo{
		FullMethod: workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
	}

	// Ensure fast requests aren't logged.
	_, err := s.interceptor.Intercept(ctx, request, info, fastHandler)
	s.NoError(err)

	// Ensure slow requests are logged.
	expectedMsg := "Slow gRPC call"
	s.logger.EXPECT().Warn(gomock.Eq(expectedMsg), gomock.Any()).Times(1)
	_, err = s.interceptor.Intercept(ctx, request, info, slowHandler)
	s.NoError(err)

	// Slow request without parameters set.
	s.logger.EXPECT().Warn(gomock.Eq(expectedMsg), gomock.Any()).Times(1)
	_, err = s.interceptor.Intercept(ctx, &workflowservice.DescribeWorkflowExecutionRequest{}, info, slowHandler)
	s.NoError(err)

	// Nil request bodies.
	s.logger.EXPECT().Warn(gomock.Eq(expectedMsg), gomock.Any()).Times(1)
	_, err = s.interceptor.Intercept(ctx, nil, info, slowHandler)
	s.NoError(err)

	// Unknown request bodies.
	s.logger.EXPECT().Warn(gomock.Eq(expectedMsg), gomock.Any()).Times(1)
	_, err = s.interceptor.Intercept(ctx, &struct{}{}, info, slowHandler)
	s.NoError(err)

	// Ensure poll requests, or other expected-slow requests, aren't logged.
	info.FullMethod = workflowservice.WorkflowService_PollWorkflowExecutionUpdate_FullMethodName
	_, err = s.interceptor.Intercept(ctx, nil, info, slowHandler)
	s.NoError(err)
}

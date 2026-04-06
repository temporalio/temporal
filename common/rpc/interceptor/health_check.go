package interceptor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	chasmProtoPrefix   = "temporal/server/chasm/lib/"
	serviceProtoSuffix = "/service.proto"
)

type (

	// HealthCheckInterceptor is a gRPC interceptor that records health metrics
	HealthCheckInterceptor struct {
		healthSignalAggregator HealthSignalAggregator
	}

	// HealthSignalAggregator interface for aggregating health signals
	HealthSignalAggregator interface {
		Record(latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
	}

	// HealthSignalAggregatorImpl implements HealthSignalAggregator
	healthSignalAggregatorImpl struct {
		status int32

		aggregatorEnabled dynamicconfig.BoolPropertyFn

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		logger log.Logger
	}
)

// excludedAPIs maps full method names to true if they should be excluded from health signals.
// This includes both long-polling APIs and system APIs.
// Built lazily on first use from proto method options.
var (
	excludedAPIs     map[string]bool
	excludedAPIsOnce sync.Once
)

func initExcludedAPIs() {
	excludedAPIs = make(map[string]bool)
	excludedCategories := map[commonspb.ApiCategory]bool{
		commonspb.API_CATEGORY_LONG_POLL: true,
		commonspb.API_CATEGORY_SYSTEM:    true,
	}

	// Process HistoryService explicitly.
	processServiceFile(historyservice.File_temporal_server_api_historyservice_v1_service_proto, excludedCategories)

	// Auto-detect all registered chasm/lib service files.
	// New services under chasm/lib are picked up automatically without code changes here.
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		path := string(fd.Path())
		if strings.HasPrefix(path, chasmProtoPrefix) && strings.HasSuffix(path, serviceProtoSuffix) {
			processServiceFile(fd, excludedCategories)
		}
		return true
	})
}

// processServiceFile enumerates all methods in a service file and adds excluded categories to excludedAPIs.
func processServiceFile(file protoreflect.FileDescriptor, excludedCategories map[commonspb.ApiCategory]bool) {
	services := file.Services()
	for i := 0; i < services.Len(); i++ {
		service := services.Get(i)
		methods := service.Methods()
		for j := 0; j < methods.Len(); j++ {
			method := methods.Get(j)
			opts, ok := method.Options().(*descriptorpb.MethodOptions)
			if ok && proto.HasExtension(opts, commonspb.E_ApiCategory) {
				categoryOpts, ok := proto.GetExtension(opts, commonspb.E_ApiCategory).(*commonspb.ApiCategoryOptions)
				if ok && categoryOpts != nil && excludedCategories[categoryOpts.GetCategory()] {
					fullMethod := fmt.Sprintf("/%s/%s", service.FullName(), method.Name())
					excludedAPIs[fullMethod] = true
				}
			}
		}
	}
}

// isExcludedAPI checks if an API is marked as a non-standard API via proto options.
func isExcludedAPI(fullMethod string) bool {
	excludedAPIsOnce.Do(initExcludedAPIs)
	return excludedAPIs[fullMethod]
}

// NewHealthCheckInterceptor creates a new health check interceptor
func NewHealthCheckInterceptor(healthSignalAggregator HealthSignalAggregator) *HealthCheckInterceptor {
	return &HealthCheckInterceptor{
		healthSignalAggregator: healthSignalAggregator,
	}
}

// UnaryIntercept implements the gRPC unary interceptor interface
func (h *HealthCheckInterceptor) UnaryIntercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	startTime := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(startTime)

	// Skip health signal recording for non-standard APIs
	if isExcludedAPI(info.FullMethod) {
		return resp, err
	}

	if IsLongPollGetWorkflowExecutionHistoryRequest(req) {
		return resp, err
	}

	if IsLongPollDescribeActivityExecutionRequest(req) {
		return resp, err
	}

	// Record health signal for standard APIs
	h.healthSignalAggregator.Record(elapsed, err)
	return resp, err
}

// NewHealthSignalAggregator creates a new instance of HealthSignalAggregatorImpl
func NewHealthSignalAggregator(
	logger log.Logger,
	aggregatorEnabled dynamicconfig.BoolPropertyFn,
	windowSize time.Duration,
	maxBufferSize int,
) *healthSignalAggregatorImpl {
	ret := &healthSignalAggregatorImpl{
		logger:            logger,
		aggregatorEnabled: aggregatorEnabled,
		latencyAverage:    aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		errorRatio:        aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
	}
	return ret
}

func (s *healthSignalAggregatorImpl) Record(latency time.Duration, err error) {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
		return
	}
	s.latencyAverage.Record(latency.Milliseconds())

	if isUnhealthyError(err) {
		s.errorRatio.Record(1)
	} else {
		s.errorRatio.Record(0)
	}
}

func (s *healthSignalAggregatorImpl) AverageLatency() float64 {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
	}
	return s.latencyAverage.Average()
}

func (s *healthSignalAggregatorImpl) ErrorRatio() float64 {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
	}
	return s.errorRatio.Average()
}

func isUnhealthyError(err error) bool {
	if err == nil {
		return false
	}
	if common.IsContextCanceledErr(err) {
		return true
	}
	if common.IsContextDeadlineExceededErr(err) {
		return true
	}

	switch err.(type) {
	case *serviceerror.Unavailable,
		*serviceerror.DeadlineExceeded,
		*serviceerror.Canceled:
		return true
	}
	return false
}

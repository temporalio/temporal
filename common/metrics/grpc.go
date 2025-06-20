package metrics

import (
	"context"
	"sync"

	metricsspb "go.temporal.io/server/api/metrics/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	metricsContextKey struct{}

	// metricsContext is used to propagate metrics across single gRPC call within server
	metricsContext struct {
		sync.Mutex
		CountersInt map[string]int64
	}
)

var (
	// "-bin" suffix is a reserved in gRPC that signals that metadata string value is actually a byte data
	// If trailer key has such a suffix, value will be base64 encoded.
	metricsTrailerKey = "metrics-trailer-bin"
	metricsCtxKey     = metricsContextKey{}
)

// NewServerMetricsContextInjectorInterceptor returns grpc server interceptor that adds metrics context to golang
// context.
func NewServerMetricsContextInjectorInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctxWithMetricsBaggage := AddMetricsContext(ctx)
		return handler(ctxWithMetricsBaggage, req)
	}
}

// NewClientMetricsTrailerPropagatorInterceptor returns grpc client interceptor that injects metrics received in trailer
// into metrics context.
func NewClientMetricsTrailerPropagatorInterceptor(logger log.Logger) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var trailer metadata.MD
		optsWithTrailer := append(opts, grpc.Trailer(&trailer))
		err := invoker(ctx, method, req, reply, cc, optsWithTrailer...)

		baggageStrings := trailer.Get(metricsTrailerKey)
		if len(baggageStrings) == 0 {
			return err
		}

		for _, baggageString := range baggageStrings {
			baggageBytes := []byte(baggageString)
			metricsBaggage := &metricsspb.Baggage{}
			unmarshalErr := metricsBaggage.Unmarshal(baggageBytes)
			if unmarshalErr != nil {
				logger.Error("unable to unmarshal metrics baggage from trailer", tag.Error(unmarshalErr))
				continue
			}
			for counterName, counterValue := range metricsBaggage.CountersInt {
				ContextCounterAdd(ctx, counterName, counterValue)
			}
		}

		return err
	}
}

// NewServerMetricsTrailerPropagatorInterceptor returns grpc server interceptor that injects metrics from context into
// gRPC trailer.
func NewServerMetricsTrailerPropagatorInterceptor(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// we want to return original handler response, so don't override err
		resp, err := handler(ctx, req)

		select {
		case <-ctx.Done():
			return resp, err
		default:
		}

		metricsCtx := getMetricsContext(ctx)
		if metricsCtx == nil {
			return resp, err
		}

		metricsBaggage := &metricsspb.Baggage{CountersInt: make(map[string]int64)}

		metricsCtx.Lock()
		for k, v := range metricsCtx.CountersInt {
			metricsBaggage.CountersInt[k] = v
		}
		metricsCtx.Unlock()

		bytes, marshalErr := metricsBaggage.Marshal()
		if marshalErr != nil {
			logger.Error("unable to marshal metric baggage", tag.Error(marshalErr))
		}

		md := metadata.Pairs(metricsTrailerKey, string(bytes))

		marshalErr = grpc.SetTrailer(ctx, md)
		if marshalErr != nil {
			logger.Error("unable to add metrics baggage to gRPC trailer", tag.Error(marshalErr))
		}

		return resp, err
	}
}

// getMetricsContext extracts metrics context from golang context.
func getMetricsContext(ctx context.Context) *metricsContext {
	metricsCtx := ctx.Value(metricsCtxKey)
	if metricsCtx == nil {
		return nil
	}

	return metricsCtx.(*metricsContext)
}

func AddMetricsContext(ctx context.Context) context.Context {
	metricsCtx := &metricsContext{}
	return context.WithValue(ctx, metricsCtxKey, metricsCtx)
}

// ContextCounterAdd adds value to counter within metrics context.
func ContextCounterAdd(ctx context.Context, name string, value int64) bool {
	metricsCtx := getMetricsContext(ctx)

	if metricsCtx == nil {
		return false
	}

	metricsCtx.Lock()
	defer metricsCtx.Unlock()

	if metricsCtx.CountersInt == nil {
		metricsCtx.CountersInt = make(map[string]int64)
	}

	val := metricsCtx.CountersInt[name]
	val += value
	metricsCtx.CountersInt[name] = val

	return true
}

// ContextCounterGet returns value and true if successfully retrieved value
func ContextCounterGet(ctx context.Context, name string) (int64, bool) {
	metricsCtx := getMetricsContext(ctx)

	if metricsCtx == nil {
		return 0, false
	}

	metricsCtx.Lock()
	defer metricsCtx.Unlock()

	if metricsCtx.CountersInt == nil {
		return 0, false
	}

	result := metricsCtx.CountersInt[name]
	return result, true
}

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
	"sync"

	metricspb "go.temporal.io/server/api/metrics/v1"
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
			metricsBaggage := &metricspb.Baggage{}
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

		metricsBaggage := &metricspb.Baggage{CountersInt: make(map[string]int64)}

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

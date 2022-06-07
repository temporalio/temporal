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

package telemetry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdkmetricexp "go.opentelemetry.io/otel/sdk/metric/export"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const (
	// the following defaults were taken from the grpc docs as of grpc v1.46.
	// they are not available programatically

	defaultReadBufferSize    = 32 * 1024
	defaultWriteBufferSize   = 32 * 1024
	defaultMinConnectTimeout = 10 * time.Second

	// the following defaults were taken from the otel library as of v1.7.
	// they are not available programatically

	retryDefaultEnabled         = true
	retryDefaultInitialInterval = 5 * time.Second
	retryDefaultMaxInterval     = 30 * time.Second
	retryDefaultMaxElapsedTime  = 1 * time.Minute
)

type (
	metadata struct {
		Name   string
		Labels map[string]string
	}

	connection struct {
		Kind     string
		Metadata metadata
		Spec     interface{} `yaml:"-"`
	}

	grpcconn struct {
		Endpoint      string
		Block         bool
		ConnectParams struct {
			MinConnectTimeout time.Duration `yaml:"min_connect_timeout"`
			Backoff           struct {
				BaseDelay  time.Duration `yaml:"base_delay"`
				Multiplier float64
				Jitter     float64
				MaxDelay   time.Duration `yaml:"max_delay"`
			}
		} `yaml:"connect_params"`
		UserAgent       string `yaml:"user_agent"`
		ReadBufferSize  int    `yaml:"read_buffer_size"`
		WriteBufferSize int    `yaml:"write_buffer_size"`
		Authority       string
		Insecure        bool

		cc *grpc.ClientConn
	}

	exporter struct {
		Kind struct {
			Signal   string
			Model    string
			Protocol string
		}
		Metadata metadata
		Spec     interface{} `yaml:"-"`
	}

	otlpGrpcExporter struct {
		ConnectionName string `yaml:"connection_name"`
		Connection     grpcconn
		Headers        map[string]string
		Timeout        time.Duration
		Retry          struct {
			Enabled         bool
			InitialInterval time.Duration `yaml:"initial_interval"`
			MaxInterval     time.Duration `yaml:"max_interval"`
			MaxElapsedTime  time.Duration `yaml:"max_elapsed_time"`
		}
	}

	otlpGrpcSpanExporter struct {
		otlpGrpcExporter `yaml:",inline"`
	}
	otlpGrpcMetricExporter struct {
		otlpGrpcExporter `yaml:",inline"`
	}

	exportConfig struct {
		Connections []connection
		Exporters   []exporter
	}

	// sharedConnSpanExporter and sharedConnMetricExporter exist to wrap a span
	// exporter that uses a shared *grpc.ClientConn so that the grpc.Dial call
	// doesn't happen until Start() is called. Without this wrapper the
	// grpc.ClientConn (which can only be created via grpc.Dial or
	// grpc.DialContext) would need to exist at _construction_ time, meaning
	// that we would need to dial at construction rather then during the start
	// phase.

	sharedConnSpanExporter struct {
		baseOpts []otlptracegrpc.Option
		dialer   interface {
			Dial(context.Context) (*grpc.ClientConn, error)
		}
		startOnce sync.Once
		otelsdktrace.SpanExporter
	}

	sharedConnMetricExporter struct {
		baseOpts []otlpmetricgrpc.Option
		dialer   interface {
			Dial(context.Context) (*grpc.ClientConn, error)
		}
		startOnce sync.Once
		otelsdkmetricexp.Exporter
	}

	// ExportConfig represents YAML structured configuration for a set of OTEL
	// trace/span/log exporters.
	ExportConfig struct {
		inner exportConfig `yaml:",inline"`
	}
)

// UnmarshalYAML loads the state of an ExportConfig from parsed YAML
func (ec *ExportConfig) UnmarshalYAML(n *yaml.Node) error {
	return n.Decode(&ec.inner)
}

func (ec *ExportConfig) SpanExporters() ([]otelsdktrace.SpanExporter, error) {
	return ec.inner.SpanExporters()
}

func (ec *ExportConfig) MetricExporters() ([]otelsdkmetricexp.Exporter, error) {
	return ec.inner.MetricExporters()
}

// Dial returns the cached *grpc.ClientConn instance or creates a new one,
// caches and then returns it. This function is not threadsafe.
func (g *grpcconn) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	var err error
	if g.cc == nil {
		g.cc, err = grpc.DialContext(ctx, g.Endpoint, g.dialOpts()...)
	}
	return g.cc, err
}

func (g *grpcconn) dialOpts() []grpc.DialOption {
	out := []grpc.DialOption{
		grpc.WithReadBufferSize(coalesce(g.ReadBufferSize, defaultReadBufferSize)),
		grpc.WithWriteBufferSize(coalesce(g.WriteBufferSize, defaultWriteBufferSize)),
		grpc.WithUserAgent(g.UserAgent),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: coalesce(g.ConnectParams.MinConnectTimeout, defaultMinConnectTimeout),
			Backoff: backoff.Config{
				BaseDelay:  coalesce(g.ConnectParams.Backoff.BaseDelay, backoff.DefaultConfig.BaseDelay),
				MaxDelay:   coalesce(g.ConnectParams.Backoff.MaxDelay, backoff.DefaultConfig.MaxDelay),
				Jitter:     coalesce(g.ConnectParams.Backoff.Jitter, backoff.DefaultConfig.Jitter),
				Multiplier: coalesce(g.ConnectParams.Backoff.Multiplier, backoff.DefaultConfig.Multiplier),
			},
		}),
	}
	if g.Insecure {
		out = append(out, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if g.Block {
		out = append(out, grpc.WithBlock())
	}
	if g.Authority != "" {
		out = append(out, grpc.WithAuthority(g.Authority))
	}
	return out
}

// SpanExporters builds the set of OTEL SpanExporter objects defined by the YAML
// unmarshaled into this ExportConfig object. The returned SpanExporters have
// not been started.
func (ec *exportConfig) SpanExporters() ([]otelsdktrace.SpanExporter, error) {
	out := make([]otelsdktrace.SpanExporter, 0, len(ec.Exporters))
	for _, expcfg := range ec.Exporters {
		if !strings.HasPrefix(expcfg.Kind.Signal, "trace") {
			continue
		}
		switch spec := expcfg.Spec.(type) {
		case *otlpGrpcSpanExporter:
			spanexp, err := ec.buildOtlpGrpcSpanExporter(spec)
			if err != nil {
				return nil, err
			}
			out = append(out, spanexp)
		default:
			return nil, fmt.Errorf("unsupported span exporter type: %T", spec)
		}
	}
	return out, nil
}

func (ec *exportConfig) MetricExporters() ([]otelsdkmetricexp.Exporter, error) {
	out := make([]otelsdkmetricexp.Exporter, 0, len(ec.Exporters))
	for _, expcfg := range ec.Exporters {
		if !strings.HasPrefix(expcfg.Kind.Signal, "metric") {
			continue
		}
		switch spec := expcfg.Spec.(type) {
		case *otlpGrpcMetricExporter:
			metricexp, err := ec.buildOtlpGrpcMetricExporter(spec)
			if err != nil {
				return nil, err
			}
			out = append(out, metricexp)
		default:
			return nil, fmt.Errorf("unsupported metric exporter type: %T", spec)
		}
	}
	return out, nil

}

func (ec *exportConfig) buildOtlpGrpcMetricExporter(
	cfg *otlpGrpcMetricExporter,
) (otelsdkmetricexp.Exporter, error) {
	dopts := cfg.Connection.dialOpts()
	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(cfg.Connection.Endpoint),
		otlpmetricgrpc.WithHeaders(cfg.Headers),
		otlpmetricgrpc.WithTimeout(coalesce(cfg.Timeout, 10*time.Second)),
		otlpmetricgrpc.WithDialOption(dopts...),
		otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
			Enabled:         coalesce(cfg.Retry.Enabled, retryDefaultEnabled),
			InitialInterval: coalesce(cfg.Retry.InitialInterval, retryDefaultInitialInterval),
			MaxInterval:     coalesce(cfg.Retry.MaxInterval, retryDefaultMaxInterval),
			MaxElapsedTime:  coalesce(cfg.Retry.MaxElapsedTime, retryDefaultMaxElapsedTime),
		}),
	}

	// work around https://github.com/open-telemetry/opentelemetry-go/issues/2940
	if cfg.Connection.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}

	if cfg.ConnectionName == "" {
		return otlpmetricgrpc.NewUnstarted(opts...), nil
	}

	conncfg, ok := ec.findNamedGrpcConnCfg(cfg.ConnectionName)
	if !ok {
		return nil, fmt.Errorf("OTEL exporter connection %q not found", cfg.ConnectionName)
	}
	return &sharedConnMetricExporter{
		baseOpts: opts,
		dialer:   conncfg,
	}, nil
}

func (ec *exportConfig) buildOtlpGrpcSpanExporter(
	cfg *otlpGrpcSpanExporter,
) (otelsdktrace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Connection.Endpoint),
		otlptracegrpc.WithHeaders(cfg.Headers),
		otlptracegrpc.WithTimeout(coalesce(cfg.Timeout, 10*time.Second)),
		otlptracegrpc.WithDialOption(cfg.Connection.dialOpts()...),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         coalesce(cfg.Retry.Enabled, retryDefaultEnabled),
			InitialInterval: coalesce(cfg.Retry.InitialInterval, retryDefaultInitialInterval),
			MaxInterval:     coalesce(cfg.Retry.MaxInterval, retryDefaultMaxInterval),
			MaxElapsedTime:  coalesce(cfg.Retry.MaxElapsedTime, retryDefaultMaxElapsedTime),
		}),
	}

	// work around https://github.com/open-telemetry/opentelemetry-go/issues/2940
	if cfg.Connection.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	if cfg.ConnectionName == "" {
		return otlptracegrpc.NewUnstarted(opts...), nil
	}

	conncfg, ok := ec.findNamedGrpcConnCfg(cfg.ConnectionName)
	if !ok {
		return nil, fmt.Errorf("OTEL exporter connection %q not found", cfg.ConnectionName)
	}
	return &sharedConnSpanExporter{
		baseOpts: opts,
		dialer:   conncfg,
	}, nil
}

// Start initiates the connection to an upstream grpc OTLP server
func (scse *sharedConnSpanExporter) Start(ctx context.Context) error {
	var err error
	scse.startOnce.Do(func() {
		var cc *grpc.ClientConn
		cc, err = scse.dialer.Dial(ctx)
		if err != nil {
			return
		}
		opts := append(scse.baseOpts, otlptracegrpc.WithGRPCConn(cc))
		scse.SpanExporter, err = otlptracegrpc.New(ctx, opts...)
	})
	return err
}

// Start initiates the connection to an upstream grpc OTLP server
func (scme *sharedConnMetricExporter) Start(ctx context.Context) error {
	var err error
	scme.startOnce.Do(func() {
		var cc *grpc.ClientConn
		cc, err = scme.dialer.Dial(ctx)
		if err != nil {
			return
		}
		opts := append(scme.baseOpts, otlpmetricgrpc.WithGRPCConn(cc))
		scme.Exporter, err = otlpmetricgrpc.New(ctx, opts...)
	})
	return err
}

func (ec *exportConfig) findNamedGrpcConnCfg(name string) (*grpcconn, bool) {
	if name == "" {
		return nil, false
	}
	for _, conn := range ec.Connections {
		if gconn, ok := conn.Spec.(*grpcconn); ok && conn.Metadata.Name == name {
			return gconn, true
		}
	}
	return nil, false
}

// UnmarshalYAML loads the state of a generic connection from parsed YAML
func (c *connection) UnmarshalYAML(n *yaml.Node) error {
	type conn connection
	type overlay struct {
		*conn `yaml:",inline"`
		Spec  yaml.Node `yaml:"spec"`
	}
	obj := overlay{conn: (*conn)(c)}
	err := n.Decode(&obj)
	if err != nil {
		return err
	}
	switch c.Kind {
	case "grpc":
		c.Spec = &grpcconn{}
	default:
		return fmt.Errorf("unsupported connection kind: %q", c.Kind)
	}
	return obj.Spec.Decode(c.Spec)
}

// UnmarshalYAML loads the state of a generic exporter from parsed YAML
func (e *exporter) UnmarshalYAML(n *yaml.Node) error {
	type exp exporter
	type overlay struct {
		*exp `yaml:",inline"`
		Spec yaml.Node `yaml:"spec"`
	}
	obj := overlay{exp: (*exp)(e)}
	err := n.Decode(&obj)
	if err != nil {
		return err
	}
	descriptor := fmt.Sprintf("%v+%v+%v", e.Kind.Signal, e.Kind.Model, e.Kind.Protocol)
	switch descriptor {
	case "traces+otlp+grpc", "trace+otlp+grpc":
		e.Spec = new(otlpGrpcSpanExporter)
	case "metrics+otlp+grpc", "metric+otlp+grpc":
		e.Spec = new(otlpGrpcMetricExporter)
	default:
		return fmt.Errorf(
			"unsupported exporter kind: signal=%q; model=%q; protocol=%q",
			e.Kind.Signal,
			e.Kind.Model,
			e.Kind.Protocol,
		)
	}
	return obj.Spec.Decode(e.Spec)
}

func coalesce[T comparable](vals ...T) T {
	var zero T
	for _, v := range vals {
		if v != zero {
			return v
		}
	}
	return zero
}

package telemetry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

type (
	metadata struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	}

	connection struct {
		Kind     string      `yaml:"kind"`
		Metadata metadata    `yaml:"metadata"`
		Spec     interface{} `yaml:"-"`
	}

	grpcconn struct {
		Endpoint      string `yaml:"endpoint"`
		Block         bool   `yaml:"block"`
		ConnectParams struct {
			MinConnectTimeout time.Duration `yaml:"min_connect_timeout"`
			Backoff           struct {
				BaseDelay  time.Duration `yaml:"base_delay"`
				Multiplier float64       `yaml:"multiplier"`
				Jitter     float64       `yaml:"jitter"`
				MaxDelay   time.Duration `yaml:"max_delay"`
			} `yaml:"backoff"`
		} `yaml:"connect_params"`
		UserAgent       string `yaml:"user_agent"`
		ReadBufferSize  int    `yaml:"read_buffer_size"`
		WriteBufferSize int    `yaml:"write_buffer_size"`
		Authority       string `yaml:"authority"`
		Insecure        bool   `yaml:"insecure"`

		cc *grpc.ClientConn
	}

	exporter struct {
		Kind struct {
			Signal string `yaml:"signal"`
			Model  string `yaml:"model"`
			Proto  string `yaml:"proto"`
		} `yaml:"kind"`
		Metadata metadata    `yaml:"metadata"`
		Spec     interface{} `yaml:"-"`
	}

	otlpGrpcSpanExporter struct {
		ConnectionName string            `yaml:"connection_name"`
		Connection     grpcconn          `yaml:"connection"`
		Headers        map[string]string `yaml:"headers"`
		Timeout        time.Duration     `yaml:"timeout"`
		Retry          struct {
			Enabled         bool          `yaml:"enabled"`
			InitialInterval time.Duration `yaml:"initial_interval"`
			MaxInterval     time.Duration `yaml:"max_interval"`
			MaxElapsedTime  time.Duration `yaml:"max_elapsed_time"`
		} `yaml:"retry"`
	}

	exportConfig struct {
		Connections []connection `yaml:"connections"`
		Exporters   []exporter   `yaml:"exporters"`
	}

	// ExportConfig represents YAML structured configuration for a set of OTEL
	// trace/span/log exporters.
	ExportConfig struct {
		inner exportConfig `yaml:",inline"`
	}
)

func (ec *ExportConfig) UnmarshalYAML(n *yaml.Node) error {
	return n.Decode(&ec.inner)
}

func (g *grpcconn) dial(ctx context.Context) (*grpc.ClientConn, error) {
	if g.cc != nil {
		return g.cc, nil
	}
	cc, err := grpc.DialContext(ctx, g.Endpoint, g.dialOpts()...)
	if err != nil {
		return nil, err
	}
	g.cc = cc
	return cc, nil
}

func (g *grpcconn) dialOpts() []grpc.DialOption {
	out := []grpc.DialOption{
		grpc.WithReadBufferSize(valueOrDefault(g.ReadBufferSize, 32*1024)),
		grpc.WithWriteBufferSize(valueOrDefault(g.WriteBufferSize, 32*1024)),
		grpc.WithUserAgent(g.UserAgent),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: valueOrDefault(g.ConnectParams.MinConnectTimeout, 10*time.Second),
			Backoff: backoff.Config{
				BaseDelay:  valueOrDefault(g.ConnectParams.Backoff.BaseDelay, backoff.DefaultConfig.BaseDelay),
				MaxDelay:   valueOrDefault(g.ConnectParams.Backoff.MaxDelay, backoff.DefaultConfig.MaxDelay),
				Jitter:     valueOrDefault(g.ConnectParams.Backoff.Jitter, backoff.DefaultConfig.Jitter),
				Multiplier: valueOrDefault(g.ConnectParams.Backoff.Multiplier, backoff.DefaultConfig.Multiplier),
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
func (c *ExportConfig) SpanExporters(ctx context.Context) ([]sdktrace.SpanExporter, error) {
	out := make([]sdktrace.SpanExporter, 0, len(c.inner.Exporters))
	for _, expcfg := range c.inner.Exporters {
		if !strings.HasPrefix(expcfg.Kind.Signal, "trace") {
			continue
		}
		switch spec := expcfg.Spec.(type) {
		case *otlpGrpcSpanExporter:
			spanexp, err := c.buildOtlpGrpcSpanExporter(ctx, spec)
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

func (c *ExportConfig) buildOtlpGrpcSpanExporter(
	ctx context.Context,
	cfg *otlpGrpcSpanExporter,
) (sdktrace.SpanExporter, error) {
	dopts := cfg.Connection.dialOpts()
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Connection.Endpoint),
		otlptracegrpc.WithHeaders(cfg.Headers),
		otlptracegrpc.WithTimeout(valueOrDefault(cfg.Timeout, 10*time.Second)),
		otlptracegrpc.WithDialOption(dopts...),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         valueOrDefault(cfg.Retry.Enabled, true),
			InitialInterval: valueOrDefault(cfg.Retry.InitialInterval, 5*time.Second),
			MaxInterval:     valueOrDefault(cfg.Retry.MaxInterval, 30*time.Second),
			MaxElapsedTime:  valueOrDefault(cfg.Retry.MaxElapsedTime, 1*time.Minute),
		}),
	}

	// work around https://github.com/open-telemetry/opentelemetry-go/issues/2940
	if cfg.Connection.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	if cfg.ConnectionName == "" {
		return otlptracegrpc.NewUnstarted(opts...), nil
	}

	conncfg, ok := c.findNamedGrpcConnCfg(cfg.ConnectionName)
	if !ok {
		return nil, fmt.Errorf("OTEL exporter connection %q not found", cfg.ConnectionName)
	}
	cc, err := conncfg.dial(ctx)
	if err != nil {
		return nil, err
	}
	opts = append(opts, otlptracegrpc.WithGRPCConn(cc))
	return otlptracegrpc.NewUnstarted(opts...), nil
}

func (c *ExportConfig) findNamedGrpcConnCfg(name string) (*grpcconn, bool) {
	if name == "" {
		return nil, false
	}
	for _, conn := range c.inner.Connections {
		if gconn, ok := conn.Spec.(*grpcconn); ok && conn.Metadata.Name == name {
			return gconn, true
		}
	}
	return nil, false
}

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
	descriptor := fmt.Sprintf("%v+%v+%v", e.Kind.Signal, e.Kind.Model, e.Kind.Proto)
	switch descriptor {
	case "traces+otlp+grpc", "trace+otlp+grpc":
		e.Spec = new(otlpGrpcSpanExporter)
	default:
		return fmt.Errorf(
			"unsupported exporter kind: signal=%q; model=%q; proto=%q",
			e.Kind.Signal,
			e.Kind.Model,
			e.Kind.Proto,
		)
	}
	return obj.Spec.Decode(e.Spec)
}

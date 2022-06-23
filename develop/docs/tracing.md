# Tracing Temporal Services with OTEL

The Temporal server supports ability to configure OTEL trace exporters to
support emitting spans and traces for observability. More specifically, the
server uses the [Go Open Telemetry
library](https://github.com/open-telemetry/opentelemetry-go) for instrumentation
and multi-protocol multi-model telemetry exporting. This document is intended to
help developers understand how to configure exporters and instrument their code.
A full exploration of tracing and telemetry is out of scope and the reader is
referred to [external reference
material](https://opentelemetry.io/docs/concepts/signals/traces/) and [third
party descriptions](https://lightstep.com/opentelemetry/tracing).

## Configuring

No trace exporters are configured by default and thus trace data is neither
collected nor emitted without additional configuration added to the server's
yaml configuration files. 

The server now supports a new `otel` YAML stanza which is used to configure a
set of process-wide exporters. In OpenTelemetry, the concept of an "exporter" is
abstract. The concrete implementation of an exporter is determined by a
3-tuple of values: the exporter signal, model, and protocol. In OTEL, a "signal"
is one of traces, metrics, or logs (in this document we will only deal with
traces), "model" indicates the abstract data model for the span and trace data
being exported, and the "protocol" specifies the concrete application protocol
binding for the indicated model. Temporal is known to support exporting trace
data as defined by otlp over either grpc or http.

A common configuration is to emit tracing data to an agent such as the
[otel-collector](https://opentelemetry.io/docs/collector/) running locally. To
configure such a system add the stanza below to your configuration yaml file(s).

```
otel:
  exporters:
    - kind:
        signal: traces
        model: otlp
        protocol: grpc
      spec:
        connection:
          insecure: true
          endpoint: localhost:4317
```

Another example is pointing Temporal directly at the Honeycomb hosted OTLP
collection service. To achieve such a configuration you will need an API key
from the upstream Honeycomb service and the stanza below.

```
otel:
  exporters:
    - kind:
        signal: traces
        model: otlp
        protocol: grpc
      spec:
        connection:
          endpoint: api.honeycomb.io:443
          headers:
            x-honeycomb-team: <a honeycomb API key>
```

Note that the configuration parser supports defining multiple exporters by
supplying additional `kind` and `spec` declarations. Additional configuration
fields can be found in [config_test.go](../../common/telemetry/config_test.go)
and are mostly related to the underlying gRPC client configuration (retries,
timeouts, etc).

Note that the Go OTEL SDK will also read a well-known set of environment
variables for configuration. So if you prefer setting environment variables to
writing YAML then you can use the [variables defined in the OTEL
spec](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md).
If environment variables conflict with YAML-provided configuration then the YAML
takes precedence.

## Instrumenting

While the exporter configuration described above is executed and set up at
process startup time, instrumentation code - the creation and termination of
spans - is inserted inline (like logging statements) into normal server
processing code. Spans are created by `go.opentelemetry.io/otel/trace.Tracer`
objects which are themselves created by
`go.opentelemetry.io/otel/trace.TracerProvider` instances. The `TracerProvider`
instances are bound to a single logical service and as such a single Temporal
process will have up to four such instances (for worker, mathcing, history, and
frontend services respectively). The `Tracer` object is bound to a single
logical _library_ which is different than a _service_. Consider that a history
_service_ instance might run code from the temporal common library, gRPC
library, and gocql library. 

`Tracer` and `TracerProvider` object management has been added to the server's
`fx` [DI configuration](https://github.com/temporalio/temporal/blob/f86b8d2c5f43907eaea4ad53ea082d70692c38cf/temporal/fx.go#L787-L889)
and thus they are available to be added to any fx-enabled object constructors.
Due the possibility of multiple services being coresident within a single
process, we do not use the OTEL library's capability to host and access a single
global `TracerProvider`.

By default, gRPC clients and servers are instrumented via the open source
[otelgrpc](https://github.com/open-telemetry/opentelemetry-go-contrib/tree/main/instrumentation/google.golang.org/grpc/otelgrpc)
library.

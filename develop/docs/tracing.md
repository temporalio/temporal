# Tracing Temporal Services with OTEL

The Temporal server supports ability to configure OTEL trace exporters to
support emitting spans and traces for observability. More specifically, the
server uses the [Go Open Telemetry
library](https://github.com/open-telemetry/opentelemetry-go) for instrumentation
and multi-protocol multi-model telemetry exporting. This document is intended to
help developers understand how to configure exporters and instrument their code.
A full exploration of tracing and telemetry is out of scope of this document and
the reader is referred to [external reference
material](https://opentelemetry.io/docs/concepts/signals/traces/), [third party
descriptions](https://lightstep.com/opentelemetry/tracing), and the
[specification
itself](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/overview.md#tracing-signal).

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

## Instrumentation Tips

### Follow the OTEL attribute naming guidelines

The OpenTelemetry project has published a non-normative set of [guidelines for
attribute naming](https://opentelemetry.io/docs/reference/specification/common/attribute-naming/).

If nothing else, please

1. Always check for an appropriate attribute in
   [semconv](https://pkg.go.dev/go.opentelemetry.io/otel/semconv) before
   creating your own
1. Always prefix Temporal attributes with `io.temporal`

### Create shared package-appropriate attribute keys

Do not create a single file in common for all attributes

Do not create packages _just_ for OTEL attributes

Do create a set of `attribute.Key`s in the semantically appropriate package and
re-use those to create `attribute.KeyValue`s as needed.

Do create a set of utility functions that can transform frequently used
aggregate types (Tasks, WorkflowExecutions, TaskQueues, etc) into an
`[]attribute.KeyValue`. The association of `attribute.KeyValue`s to a
`trace.Span` can be verbose in terms of the number of lines of code needed so
any reduction in that noise will be a good idea. Not to mention the consistency
benefit of sharing a single mapping function.

### Start a span in `common` or other non-service-specific code

*Q:* Given that common code can be called from any service, how can I start a span
in common library code that is bound the the appropriate service
(frontend/history/matching/worker)?

*A:* The `TracerProvider` that created the currently active Span can be retrieved
from that Span itself and the currently active Span can be received from from
the `context.Context`.

```
// DoFoo is a function in the common package
func DoFoo(ctx context.Context, x int, y string) string {
   var span trace.Span
   ctx, span = trace.SpanFromContext(ctx).TracerProvider().Tracer("go.temporal.io/server/common").Start("DoFoo")
   defer span.End()
   return fmt.Sprintf("%v-%v", y, x)
}
```

### `RecordError` does not imply Span failure

Using `Span.RecordError` is a good idea but not all errors imply failure. Thus
if you want to capture an error _and also_ capture that a span failed, you must
additionally call `Span.SetStatus(codes.Error, err.Error())`. A
`FailSpanWithError` utility function might be a good idea.

### Propagate TraceContext across things other than function calls

This is taken care of by default for gRPC calls via the otelgrpc interceptors.
However you may want to propagate tracing information between goroutines or
other places where the `context.Context` is not passed such as handoffs through
a Go channel or an external datastore. There are two broad approaches that are
applicable in different situations:

1. If the object being transferred is not externally durable (e.g. an object put
   into a Go channel but _not_ spooled to a database) then you can pull the
   `trace.SpanContext` out of the current `trace.Span` with
   `trace.SpanContextFromContext(context.Context)` or `Span.SpanContext()` and
   pass that object along with the data being transferred. The consuming side
   can restore the tracing state with
   `trace.ContextWithSpanContext(trace.SpanContext)`.
1. If the tracing state needs to be serialized, the OTEL library provides the
   [propagation](https://pkg.go.dev/go.opentelemetry.io/otel@v1.7.0/propagation)
   package to convert trace state into a more serialization-friendly type such
   as a `map[string]string`. The `propagation.TraceContext` type can be used to
   inject and extract trace state into a key-value-ish object.

```
carrier := propagation.MapCarrier(map[string]string{})
propagation.TraceContext{}.Inject(ctx, carrier)
// write the carrier object to a durable store
```
### Trace individual tasks that are processed together in batches

OpenTelemetry Spans can be _linked_ together to form a non-parent-child
relationship. One of the main use cases for linking is so that a batch process
(e.g. a database read that fills a large buffer of work items) can create Spans
for each of the individual work items it creates and those Spans can be linked
back to the parent batch Span without that span becoming their logical parent.

### Still want to log things?

Use `Span.AddEvent` to write messages that will be associated with that `Span`.
From the OTEL manual

> An event is a human-readable message on a span that represents “something happening” during it’s lifetime

// The MIT License
//
// Copyright (c) 2023 Payrails GmbH
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

package gocql

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
)

type (
	tracer interface {
		querySpan(q *query, action string) func()
		batchSpan(b *batch, action string) func()
	}

	nopTracer struct {
	}

	cassandraTracer struct {
		traceCassandra bool
	}

	gocqlTracer struct {
		session    *session
		parentSpan trace.Span
		ctx        context.Context
	}
)

func nop() {}

func (nopTracer) querySpan(*query, string) func() {
	return nop
}

func (nopTracer) batchSpan(*batch, string) func() {
	return nop
}

func (c *cassandraTracer) querySpan(q *query, operation string) func() {
	ctx := q.gocqlQuery.Context()
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("go.temporal.io/server/common").Start(
		ctx, "Query."+operation,
		trace.WithAttributes(
			attribute.String("Statement", q.gocqlQuery.Statement()),
		))
	q.gocqlQuery.WithContext(ctx)

	if c.traceCassandra {
		q.gocqlQuery.Trace(&gocqlTracer{
			session:    q.session,
			parentSpan: span,
			ctx:        ctx,
		})
	}

	return func() {
		span.End()
	}
}

func (c *cassandraTracer) batchSpan(b *batch, operation string) func() {
	ctx := b.gocqlBatch.Context()

	statements := ""
	for _, entry := range b.gocqlBatch.Entries {
		if statements != "" {
			statements += "\n"
		}

		statements += entry.Stmt
	}

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("go.temporal.io/server/common").Start(
		ctx, "Batch."+operation,
		trace.WithAttributes(
			attribute.String("Statements", statements),
		))
	b.gocqlBatch.WithContext(ctx)

	if c.traceCassandra {
		b.gocqlBatch.Trace(&gocqlTracer{
			session:    b.session,
			parentSpan: span,
			ctx:        ctx,
		})
	}

	return func() {
		span.End()
	}
}

func (g *gocqlTracer) Trace(traceId []byte) {
	session := g.session.Value.Load().(*gocql.Session)

	var (
		coordinator string
		duration    int
	)
	iter := session.Query(`SELECT coordinator, duration
                       FROM system_traces.sessions
                       WHERE session_id = ?`, traceId).Consistency(gocql.One).Iter()

	iter.Scan(&coordinator, &duration)
	if err := iter.Close(); err != nil {
		fmt.Println("Error:", err)
		return
	}

	var (
		timestamp time.Time
		activity  string
		source    string
		elapsed   int
	)

	type sourceTiming struct {
		firstSeen time.Time
		lastEvent time.Time
	}
	sourceTimes := make(map[string]*sourceTiming)

	// fmt.Printf("Tracing session %016x (coordinator: %s, duration: %v):\n",
	//      traceId, coordinator, time.Duration(duration)*time.Microsecond)

	iter = session.Query(`SELECT event_id, activity, source, source_elapsed
                       FROM system_traces.events
                       WHERE session_id = ?`, traceId).Consistency(gocql.One).Iter()

	for iter.Scan(&timestamp, &activity, &source, &elapsed) {
		// fmt.Printf("%s: %s (source: %s, elapsed: %d)\n",
		//      timestamp.Format("2006/01/02 15:04:05.999999"), activity, source, elapsed)

		times, ok := sourceTimes[source]
		if !ok {
			sourceTimes[source] = &sourceTiming{
				firstSeen: timestamp.Add(-time.Duration(elapsed) * time.Microsecond),
				lastEvent: timestamp.Add(-time.Duration(elapsed) * time.Microsecond),
			}
			times = sourceTimes[source]
		}
		eventEnd := times.firstSeen.Add(time.Duration(elapsed) * time.Microsecond)

		_, span := g.parentSpan.TracerProvider().Tracer("go.temporal.io/server/common").Start(
			g.ctx,
			activity,
			trace.WithTimestamp(times.lastEvent),
			trace.WithAttributes(
				semconv.ServiceNameKey.String("cassandra"),
				attribute.String("node", source),
			))
		span.End(trace.WithTimestamp(eventEnd))
		times.lastEvent = eventEnd
	}

	if err := iter.Close(); err != nil {
		fmt.Println("Error:", err)
	}
}

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

package gocql

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "go.temporal.io/common/persistence/nosqlplugin/cassandra/gocql"
	batchOperationName  = "batch"
	queryOperationName  = "query"
)

var (
	batchOperationAttr = semconv.DBOperationKey.String(batchOperationName)
	queryOperationAttr = semconv.DBOperationKey.String(queryOperationName)
)

type traceObserver struct {
	dbNameAttr    attribute.KeyValue
	batchSpanName string
	querySpanName string
	tracer        trace.Tracer

	nextBatchObserver gocql.BatchObserver
	nextQueryObserver gocql.QueryObserver
}

// ConfigureTracing registers tracing observers with the supplied
// gocql.ClusterConfig. Any pre-existing observers are preserved and will be
// called after the appropriate tracing listener.
func ConfigureTracing(config *gocql.ClusterConfig, tp trace.TracerProvider) {
	obs := traceObserver{

		// > In Cassandra, db.name SHOULD be set to the keyspace name
		// https://github.com/open-telemetry/opentelemetry-specification/blob/4f8d9f679d8e138693eb6fd085e6f6654012d116/specification/trace/semantic_conventions/database.md?plain=1#L179
		dbNameAttr: semconv.DBNameKey.String(config.Keyspace),

		// > If db.sql.table is not available [...] the span SHOULD be named
		// > "<db.operation> <db.name>"
		// https://github.com/open-telemetry/opentelemetry-specification/blob/4f8d9f679d8e138693eb6fd085e6f6654012d116/specification/trace/semantic_conventions/database.md?plain=1#L29
		batchSpanName:     fmt.Sprintf("%s %s", batchOperationName, config.Keyspace),
		querySpanName:     fmt.Sprintf("%s %s", queryOperationName, config.Keyspace),
		tracer:            tp.Tracer(instrumentationName),
		nextBatchObserver: config.BatchObserver,
		nextQueryObserver: config.QueryObserver,
	}
	config.BatchObserver = &obs
	config.QueryObserver = &obs
}

// ObserveBatch handles callbacks from gocql upon completion of a Cassandra
// batch operation.
func (to *traceObserver) ObserveBatch(
	ctx context.Context,
	batch gocql.ObservedBatch,
) {
	to.observe(
		ctx,
		to.batchSpanName,
		batchOperationAttr,
		batch.Start,
		batch.End,
		"",
		batch.Statements,
		batch.Host.ConnectAddress(),
		batch.Host.Port(),
		batch.Err,
	)
	if to.nextBatchObserver != nil {
		to.nextBatchObserver.ObserveBatch(ctx, batch)
	}
}

// ObserveQuery handles callbacks from gocql upon completion of a Cassandra
// query operation.
func (to *traceObserver) ObserveQuery(
	ctx context.Context,
	query gocql.ObservedQuery,
) {
	to.observe(
		ctx,
		to.querySpanName,
		queryOperationAttr,
		query.Start,
		query.End,
		query.Statement,
		nil,
		query.Host.ConnectAddress(),
		query.Host.Port(),
		query.Err,
	)
	if to.nextQueryObserver != nil {
		to.nextQueryObserver.ObserveQuery(ctx, query)
	}
}

func (to *traceObserver) observe(
	ctx context.Context,
	spanName string,
	operation attribute.KeyValue,
	start time.Time,
	end time.Time,
	statement string,
	statements []string,
	addr net.IP,
	port int,
	err error,
) {
	_, span := to.tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(start),
	)
	defer span.End(trace.WithTimestamp(end))
	if !span.IsRecording() {
		return
	}
	if len(statements) > 0 {
		statement = strings.Join(statements, "; ")
	}
	span.SetAttributes(
		semconv.DBSystemCassandra,
		to.dbNameAttr,
		operation,
		semconv.DBStatementKey.String(statement),
		semconv.NetPeerIPKey.String(addr.String()),
		semconv.NetPeerPortKey.Int(port),
	)
	span.RecordError(err)
}

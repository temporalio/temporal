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

package gocql_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	temporalgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type (
	// testObserver mocks both gocql.BatchObserver and gocql.QueryObserver
	testObserver struct {
		observeBatchFunc func(context.Context, gocql.ObservedBatch)
		observeQueryFunc func(context.Context, gocql.ObservedQuery)
	}
)

func (to testObserver) ObserveBatch(ctx context.Context, batch gocql.ObservedBatch) {
	to.observeBatchFunc(ctx, batch)
}

func (to testObserver) ObserveQuery(ctx context.Context, query gocql.ObservedQuery) {
	to.observeQueryFunc(ctx, query)
}

func testHost() *gocql.HostInfo {
	return (&gocql.HostInfo{}).SetConnectAddress(net.ParseIP("192.0.2.1"))
}

func TestObserveQuery(t *testing.T) {
	ccfg := gocql.ClusterConfig{Keyspace: t.Name()}
	spanrec := tracetest.NewInMemoryExporter()
	tp := otelsdk.NewTracerProvider(otelsdk.WithSyncer(spanrec))

	temporalgocql.ConfigureTracing(&ccfg, tp)

	host := testHost()
	query := gocql.ObservedQuery{
		Keyspace:  ccfg.Keyspace,
		Statement: "select * from foo",
		Start:     time.Now().Add(-5 * time.Second),
		End:       time.Now(),
		Host:      host,
		Err:       errors.New("error in " + t.Name()),
	}
	ccfg.QueryObserver.ObserveQuery(context.TODO(), query)

	spans := spanrec.GetSpans().Snapshots()

	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "query "+query.Keyspace, span.Name())
	require.Equal(t, trace.SpanKindClient, span.SpanKind())
	require.Equal(t, query.Start, span.StartTime())
	require.Equal(t, query.End, span.EndTime())
	require.Contains(t, span.Attributes(),
		semconv.DBStatementKey.String(query.Statement))
	require.Contains(t, span.Attributes(),
		semconv.NetPeerIPKey.String(host.ConnectAddress().String()))
	require.Len(t, span.Events(), 1)
	require.Contains(t, span.Events()[0].Attributes,
		semconv.ExceptionMessageKey.String(query.Err.Error()))
}

func TestObserveBatch(t *testing.T) {
	ccfg := gocql.ClusterConfig{Keyspace: t.Name()}
	spanrec := tracetest.NewInMemoryExporter()
	tp := otelsdk.NewTracerProvider(otelsdk.WithSyncer(spanrec))

	temporalgocql.ConfigureTracing(&ccfg, tp)

	host := testHost()
	batch := gocql.ObservedBatch{
		Keyspace:   ccfg.Keyspace,
		Statements: []string{"select * from foo", "update bar"},
		Start:      time.Now().Add(-5 * time.Second),
		End:        time.Now(),
		Host:       host,
		Err:        errors.New("error in " + t.Name()),
	}
	ccfg.BatchObserver.ObserveBatch(context.TODO(), batch)

	spans := spanrec.GetSpans().Snapshots()

	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "batch "+batch.Keyspace, span.Name())
	require.Equal(t, trace.SpanKindClient, span.SpanKind())
	require.Equal(t, batch.Start, span.StartTime())
	require.Equal(t, batch.End, span.EndTime())
	require.Contains(t, span.Attributes(),
		semconv.DBStatementKey.String(strings.Join(batch.Statements, "; ")))
	require.Contains(t, span.Attributes(),
		semconv.NetPeerIPKey.String(host.ConnectAddress().String()))
	require.Len(t, span.Events(), 1)
	require.Contains(t, span.Events()[0].Attributes,
		semconv.ExceptionMessageKey.String(batch.Err.Error()))
}

func TestObserverChaining(t *testing.T) {
	var batchObserverCalled, queryObserverCalled bool
	ccfg := gocql.ClusterConfig{
		Keyspace: t.Name(),

		// these next two observers should be called even though we're
		// installing the tracing observers into the configuration.

		BatchObserver: testObserver{
			observeBatchFunc: func(context.Context, gocql.ObservedBatch) {
				batchObserverCalled = true
			},
		},
		QueryObserver: testObserver{
			observeQueryFunc: func(context.Context, gocql.ObservedQuery) {
				queryObserverCalled = true
			},
		},
	}

	temporalgocql.ConfigureTracing(&ccfg, trace.NewNoopTracerProvider())

	ccfg.BatchObserver.ObserveBatch(
		context.Background(),
		gocql.ObservedBatch{Host: testHost()},
	)
	ccfg.QueryObserver.ObserveQuery(
		context.Background(),
		gocql.ObservedQuery{Host: testHost()},
	)

	require.True(t, batchObserverCalled)
	require.True(t, queryObserverCalled)
}

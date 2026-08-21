package gocql

import (
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Note: this file defines the minimal interface that is needed by Temporal's cassandra
// persistence implementation and should be implemented for all gocql libraries if
// they need to be used.
// Please add more methods to the interface if needed by the cassandra implementation.

type (
	// Session is the interface for interacting with the database.
	Session interface {
		Query(string, ...any) Query
		NewBatch(BatchType) *Batch
		AwaitSchemaAgreement(ctx context.Context) error
		Close()
	}

	// Query is the interface for query object.
	Query interface {
		Exec(context.Context) error
		Scan(context.Context, ...any) error
		ScanCAS(context.Context, ...any) (bool, error)
		MapScan(context.Context, map[string]any) error
		MapScanCAS(context.Context, map[string]any) (bool, error)
		Iter(context.Context) Iter
		PageSize(int) Query
		PageState([]byte) Query
		WithTimestamp(int64) Query
		Consistency(Consistency) Query
		Bind(...any) Query
		Idempotent(bool) Query
		SetSpeculativeExecutionPolicy(SpeculativeExecutionPolicy) Query
	}

	// Iter is the interface for executing and iterating over all resulting rows.
	Iter interface {
		Scan(...any) bool
		MapScan(map[string]any) bool
		PageState() []byte
		Close() error
	}

	// BatchType is the type of the Batch operation
	BatchType byte

	// Consistency is the consistency level used by a Query
	Consistency uint16

	// SerialConsistency is the serial consistency level used by a Query
	SerialConsistency uint16

	// SpeculativeExecutionPolicy is a gocql SpeculativeExecutionPolicy
	SpeculativeExecutionPolicy gocql.SpeculativeExecutionPolicy
)

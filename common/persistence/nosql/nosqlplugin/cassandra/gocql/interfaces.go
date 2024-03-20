package gocql

import (
	"context"

	"github.com/gocql/gocql"
)

// Note: this file defines the minimal interface that is needed by Temporal's cassandra
// persistence implementation and should be implemented for all gocql libraries if
// they need to be used.
// Please add more methods to the interface if needed by the cassandra implementation.

type (
	// Session is the interface for interacting with the database.
	Session interface {
		Query(string, ...interface{}) Query
		NewBatch(BatchType) *Batch
		ExecuteBatch(*Batch) error
		MapExecuteBatchCAS(*Batch, map[string]interface{}) (bool, Iter, error)
		AwaitSchemaAgreement(ctx context.Context) error
		Close()
	}

	// GocqlSession is an interface compatible with the concrete type *gocql.Session.
	//
	// It only declares the functions which are actually used in Temporal,
	// so that when declaring a compatible type, only what is strictly required
	// needs to be implemented.
	//
	// This is usefull when using the `CreateSessionFunc` parameter of the
	// cassandra config, in order to override the default behavior.
	GocqlSession interface {
		Query(stmt string, values ...interface{}) *gocql.Query
		NewBatch(typ gocql.BatchType) *gocql.Batch
		ExecuteBatch(batch *gocql.Batch) error
		MapExecuteBatchCAS(batch *gocql.Batch, dest map[string]interface{}) (applied bool, iter *gocql.Iter, err error)
		AwaitSchemaAgreement(ctx context.Context) error
		Close()
	}

	// Query is the interface for query object.
	Query interface {
		Exec() error
		Scan(...interface{}) error
		ScanCAS(...interface{}) (bool, error)
		MapScan(map[string]interface{}) error
		MapScanCAS(map[string]interface{}) (bool, error)
		Iter() Iter
		PageSize(int) Query
		PageState([]byte) Query
		WithContext(context.Context) Query
		WithTimestamp(int64) Query
		Consistency(Consistency) Query
		Bind(...interface{}) Query
	}

	// Iter is the interface for executing and iterating over all resulting rows.
	Iter interface {
		Scan(...interface{}) bool
		MapScan(map[string]interface{}) bool
		PageState() []byte
		Close() error
	}

	// BatchType is the type of the Batch operation
	BatchType byte

	// Consistency is the consistency level used by a Query
	Consistency uint16

	// SerialConsistency is the serial consistency level used by a Query
	SerialConsistency uint16
)

var _ GocqlSession = &gocql.Session{}

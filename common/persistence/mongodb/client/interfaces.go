// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package client

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Client wraps the MongoDB driver client to provide testability and abstraction
type Client interface {
	// Connect establishes connection to MongoDB
	Connect(ctx context.Context) error
	// Close terminates all connections
	Close(ctx context.Context) error
	// Database returns a database handle
	Database(name string) Database
	// StartSession begins a new session for transactions
	StartSession(ctx context.Context) (Session, error)
	// Ping verifies connectivity
	Ping(ctx context.Context) error
	// GetTopology detects the MongoDB deployment topology
	GetTopology(ctx context.Context) (*TopologyInfo, error)
	// NumberSessionsInProgress returns the current number of sessions in progress
	NumberSessionsInProgress() int
}

// Database wraps MongoDB database operations
type Database interface {
	// Collection returns a collection handle
	Collection(name string) Collection
	// RunCommand executes a database command
	RunCommand(ctx context.Context, runCommand interface{}) SingleResult
}

// Collection wraps MongoDB collection operations
type Collection interface {
	// FindOne executes a find command and returns a single document
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) SingleResult
	// Find executes a find command and returns multiple documents
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error)
	// InsertOne inserts a single document
	InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	// InsertMany inserts multiple documents
	InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	// UpdateOne updates a single document
	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	// UpdateMany updates multiple documents
	UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	// DeleteOne deletes a single document
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	// DeleteMany deletes multiple documents
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	// CountDocuments counts documents matching filter
	CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error)
	// Aggregate executes an aggregation pipeline
	Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (Cursor, error)
	// Indexes returns the index view for this collection
	Indexes() IndexView
}

// SingleResult wraps a single document result
type SingleResult interface {
	// Decode unmarshals the document into v
	Decode(v interface{}) error
	// Err returns any error from the operation
	Err() error
}

// Cursor wraps a MongoDB cursor for iterating results
type Cursor interface {
	// Next advances the cursor to the next document
	Next(ctx context.Context) bool
	// Decode unmarshals the current document into v
	Decode(v interface{}) error
	// All iterates the cursor and decodes each document into results
	All(ctx context.Context, results interface{}) error
	// Close closes the cursor
	Close(ctx context.Context) error
	// Err returns the last error seen by the cursor
	Err() error
}

// Session wraps MongoDB session for transaction support
type Session interface {
	// StartTransaction starts a new transaction
	StartTransaction(opts ...*options.TransactionOptions) error
	// CommitTransaction commits the transaction
	CommitTransaction(ctx context.Context) error
	// AbortTransaction aborts the transaction
	AbortTransaction(ctx context.Context) error
	// WithTransaction executes a function within a transaction
	WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error)
	// EndSession closes the session
	EndSession(ctx context.Context)
}

// IndexView wraps MongoDB index operations
type IndexView interface {
	// List returns a cursor iterating over all indexes in the collection
	List(ctx context.Context, opts ...*options.ListIndexesOptions) (Cursor, error)
	// CreateOne creates a single index
	CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error)
	// DropOne drops a single index by name
	DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) error
}

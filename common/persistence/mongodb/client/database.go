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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoDatabase implements the Database interface
type mongoDatabase struct {
	db *mongo.Database
}

func (d *mongoDatabase) Collection(name string) Collection {
	return &mongoCollection{coll: d.db.Collection(name)}
}

func (d *mongoDatabase) RunCommand(ctx context.Context, runCommand interface{}) SingleResult {
	return &mongoSingleResult{sr: d.db.RunCommand(ctx, runCommand)}
}

// mongoCollection implements the Collection interface
type mongoCollection struct {
	coll *mongo.Collection
}

func (c *mongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) SingleResult {
	return &mongoSingleResult{sr: c.coll.FindOne(ctx, filter, opts...)}
}

func (c *mongoCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error) {
	cursor, err := c.coll.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoCursor{cursor: cursor}, nil
}

func (c *mongoCollection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	return c.coll.InsertOne(ctx, document, opts...)
}

func (c *mongoCollection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return c.coll.InsertMany(ctx, documents, opts...)
}

func (c *mongoCollection) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return c.coll.UpdateOne(ctx, filter, update, opts...)
}

func (c *mongoCollection) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return c.coll.UpdateMany(ctx, filter, update, opts...)
}

func (c *mongoCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return c.coll.DeleteOne(ctx, filter, opts...)
}

func (c *mongoCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return c.coll.DeleteMany(ctx, filter, opts...)
}

func (c *mongoCollection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	return c.coll.CountDocuments(ctx, filter, opts...)
}

func (c *mongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (Cursor, error) {
	cursor, err := c.coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoCursor{cursor: cursor}, nil
}

func (c *mongoCollection) Indexes() IndexView {
	return &mongoIndexView{iv: c.coll.Indexes()}
}

// mongoSingleResult implements the SingleResult interface
type mongoSingleResult struct {
	sr *mongo.SingleResult
}

func (r *mongoSingleResult) Decode(v interface{}) error {
	return r.sr.Decode(v)
}

func (r *mongoSingleResult) Err() error {
	return r.sr.Err()
}

// mongoCursor implements the Cursor interface
type mongoCursor struct {
	cursor *mongo.Cursor
}

func (c *mongoCursor) Next(ctx context.Context) bool {
	return c.cursor.Next(ctx)
}

func (c *mongoCursor) Decode(v interface{}) error {
	return c.cursor.Decode(v)
}

func (c *mongoCursor) All(ctx context.Context, results interface{}) error {
	return c.cursor.All(ctx, results)
}

func (c *mongoCursor) Close(ctx context.Context) error {
	return c.cursor.Close(ctx)
}

func (c *mongoCursor) Err() error {
	return c.cursor.Err()
}

// mongoSession implements the Session interface
type mongoSession struct {
	session mongo.Session
}

func (s *mongoSession) StartTransaction(opts ...*options.TransactionOptions) error {
	return s.session.StartTransaction(opts...)
}

func (s *mongoSession) CommitTransaction(ctx context.Context) error {
	return s.session.CommitTransaction(ctx)
}

func (s *mongoSession) AbortTransaction(ctx context.Context) error {
	return s.session.AbortTransaction(ctx)
}

func (s *mongoSession) WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error), opts ...*options.TransactionOptions) (interface{}, error) {
	return s.session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		return fn(sessCtx)
	}, opts...)
}

func (s *mongoSession) EndSession(ctx context.Context) {
	s.session.EndSession(ctx)
}

// mongoIndexView implements the IndexView interface
type mongoIndexView struct {
	iv mongo.IndexView
}

func (iv *mongoIndexView) List(ctx context.Context, opts ...*options.ListIndexesOptions) (Cursor, error) {
	cursor, err := iv.iv.List(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoCursor{cursor: cursor}, nil
}

func (iv *mongoIndexView) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	return iv.iv.CreateOne(ctx, model, opts...)
}

func (iv *mongoIndexView) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) error {
	_, err := iv.iv.DropOne(ctx, name, opts...)
	return err
}

// Ensure unused import is avoided
var _ = bson.M{}

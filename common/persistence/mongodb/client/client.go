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
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// mongoClient implements the Client interface
type mongoClient struct {
	client *mongo.Client
}

// NewClient creates a new MongoDB client and establishes connection
func NewClient(ctx context.Context, uri string, opts ...*options.ClientOptions) (Client, error) {
	clientOpts := options.Client().ApplyURI(uri)

	// Apply additional options if provided
	if len(opts) > 0 {
		for _, opt := range opts {
			if opt != nil {
				clientOpts = opt
			}
		}
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	return &mongoClient{client: client}, nil
}

func (c *mongoClient) Connect(ctx context.Context) error {
	// Connection already established in NewClient
	return c.Ping(ctx)
}

func (c *mongoClient) Close(ctx context.Context) error {
	if err := c.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
	}
	return nil
}

func (c *mongoClient) Database(name string) Database {
	return &mongoDatabase{db: c.client.Database(name)}
}

func (c *mongoClient) StartSession(ctx context.Context) (Session, error) {
	session, err := c.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}
	return &mongoSession{session: session}, nil
}

func (c *mongoClient) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	return nil
}

func (c *mongoClient) NumberSessionsInProgress() int {
	if c.client == nil {
		return 0
	}
	return c.client.NumberSessionsInProgress()
}

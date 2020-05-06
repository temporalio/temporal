// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package blobstore

import "context"

type (
	// Client defines the interface to a blobstore client.
	Client interface {
		Put(context.Context, *PutRequest) (*PutResponse, error)
		Get(context.Context, *GetRequest) (*GetResponse, error)
		Exists(context.Context, *ExistsRequest) (*ExistsResponse, error)
		Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
		IsRetryableError(error) bool
	}

	// PutRequest is the request to Put
	PutRequest struct {
		Key  string
		Blob Blob
	}

	// PutResponse is the response from Put
	PutResponse struct{}

	// GetRequest is the request to Get
	GetRequest struct {
		Key string
	}

	// GetResponse is the response from Get
	GetResponse struct {
		Blob Blob
	}

	// ExistsRequest is the request to Exists
	ExistsRequest struct {
		Key string
	}

	// ExistsResponse is the response from Exists
	ExistsResponse struct {
		Exists bool
	}

	// DeleteRequest is the request to Delete
	DeleteRequest struct {
		Key string
	}

	// DeleteResponse is the response from Delete
	DeleteResponse struct{}

	// Blob defines a blob which can be stored and fetched from blobstore
	Blob struct {
		Tags map[string]string
		Body []byte
	}
)

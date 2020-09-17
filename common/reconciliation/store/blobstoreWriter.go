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

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/pagination"
)

type (
	blobstoreWriter struct {
		writer    pagination.Writer
		uuid      string
		extension Extension
	}
)

// NewBlobstoreWriter constructs a new blobstore writer
func NewBlobstoreWriter(
	uuid string,
	extension Extension,
	client blobstore.Client,
	flushThreshold int,
) ExecutionWriter {
	return &blobstoreWriter{
		writer: pagination.NewWriter(
			getBlobstoreWriteFn(uuid, extension, client),
			getBlobstoreShouldFlushFn(flushThreshold),
			0),
		uuid:      uuid,
		extension: extension,
	}
}

// Add adds an entity to blobstore writer
func (bw *blobstoreWriter) Add(e interface{}) error {
	return bw.writer.Add(e)
}

// Flush flushes contents of writer to blobstore.
// Only triggers flush if page contains some contents.
func (bw *blobstoreWriter) Flush() error {
	return bw.writer.FlushIfNotEmpty()
}

// FlushedKeys returns the keys that have been successfully flushed.
// Returns nil if no keys have been flushed.
func (bw *blobstoreWriter) FlushedKeys() *Keys {
	if len(bw.writer.FlushedPages()) == 0 {
		return nil
	}
	return &Keys{
		UUID:      bw.uuid,
		MinPage:   bw.writer.FirstFlushedPage().(int),
		MaxPage:   bw.writer.LastFlushedPage().(int),
		Extension: bw.extension,
	}
}

func getBlobstoreWriteFn(
	uuid string,
	extension Extension,
	client blobstore.Client,
) pagination.WriteFn {
	return func(page pagination.Page) (pagination.PageToken, error) {
		blobIndex := page.CurrentToken.(int)
		key := pageNumberToKey(uuid, extension, blobIndex)
		buffer := &bytes.Buffer{}
		for _, e := range page.Entities {
			data, err := json.Marshal(e)
			if err != nil {
				return nil, err
			}
			buffer.Write(data)
			buffer.Write(SeparatorToken)
		}
		req := &blobstore.PutRequest{
			Key: key,
			Blob: blobstore.Blob{
				Body: buffer.Bytes(),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()
		if _, err := client.Put(ctx, req); err != nil {
			return nil, err
		}
		return blobIndex + 1, nil
	}
}

func getBlobstoreShouldFlushFn(
	flushThreshold int,
) pagination.ShouldFlushFn {
	return func(page pagination.Page) bool {
		return len(page.Entities) > flushThreshold
	}
}

func pageNumberToKey(uuid string, extension Extension, pageNum int) string {
	return fmt.Sprintf("%v_%v.%v", uuid, pageNum, extension)
}

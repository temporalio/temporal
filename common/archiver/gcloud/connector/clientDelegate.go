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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination clientDelegate_mock.go

package connector

import (
	"context"
	"os"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type (
	// GcloudStorageClient is an interface that expose some methods from gcloud storage client
	GcloudStorageClient interface {
		Bucket(URI string) BucketHandleWrapper
	}

	clientDelegate struct {
		nativeClient *storage.Client
	}
)

type (
	// BucketHandleWrapper is an interface that expose some methods from gcloud storage bucket
	BucketHandleWrapper interface {
		Object(name string) ObjectHandleWrapper
		Objects(ctx context.Context, q *storage.Query) ObjectIteratorWrapper
		Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	}

	bucketDelegate struct {
		bucket *storage.BucketHandle
	}
)

type (
	// ObjectHandleWrapper is an interface that expose some methods from gcloud storage object
	ObjectHandleWrapper interface {
		NewWriter(ctx context.Context) WriterWrapper
		NewReader(ctx context.Context) (ReaderWrapper, error)
		Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	}

	objectDelegate struct {
		object *storage.ObjectHandle
	}
)

type (
	// WriterWrapper is an interface that expose some methods from gcloud storage writer
	WriterWrapper interface {
		Close() error
		Write(p []byte) (n int, err error)
		CloseWithError(err error) error
	}

	writerDelegate struct {
		writer *storage.Writer
	}
)

type (
	// ReaderWrapper is an interface that expose some methods from gcloud storage reader
	ReaderWrapper interface {
		Close() error
		Read(p []byte) (int, error)
	}

	readerDelegate struct {
		reader *storage.Reader
	}
)

type (
	// ObjectIteratorWrapper is an interface that expose some methods from gcloud storage objectIterator
	ObjectIteratorWrapper interface {
		Next() (*storage.ObjectAttrs, error)
	}
)

func newDefaultClientDelegate(ctx context.Context) (*clientDelegate, error) {
	nativeClient, err := storage.NewClient(ctx)
	return &clientDelegate{nativeClient: nativeClient}, err
}

func newClientDelegateWithCredentials(ctx context.Context, credentialsPath string) (*clientDelegate, error) {

	jsonKey, err := os.ReadFile(credentialsPath)
	if err != nil {
		return newDefaultClientDelegate(ctx)
	}

	conf, err := google.JWTConfigFromJSON(jsonKey, storage.ScopeFullControl)
	if err != nil {
		return newDefaultClientDelegate(ctx)
	}

	nativeClient, err := storage.NewClient(ctx, option.WithTokenSource(conf.TokenSource(ctx)))
	return &clientDelegate{nativeClient: nativeClient}, err
}

// Bucket returns a BucketHandle, which provides operations on the named bucket.
// This call does not perform any network operations.
//
// The supplied name must contain only lowercase letters, numbers, dashes,
// underscores, and dots. The full specification for valid bucket names can be
// found at:
//   https://cloud.google.com/storage/docs/bucket-naming
func (c *clientDelegate) Bucket(bucketName string) BucketHandleWrapper {
	return &bucketDelegate{bucket: c.nativeClient.Bucket(bucketName)}
}

// Object returns an ObjectHandle, which provides operations on the named object.
// This call does not perform any network operations.
//
// name must consist entirely of valid UTF-8-encoded runes. The full specification
// for valid object names can be found at:
//   https://cloud.google.com/storage/docs/bucket-naming
func (b *bucketDelegate) Object(name string) ObjectHandleWrapper {
	return &objectDelegate{object: b.bucket.Object(name)}
}

// Objects returns an iterator over the objects in the bucket that match the Query q.
// If q is nil, no filtering is done.
func (b *bucketDelegate) Objects(ctx context.Context, q *storage.Query) ObjectIteratorWrapper {
	return b.bucket.Objects(ctx, q)
}

// Attrs returns the metadata for the bucket.
func (b *bucketDelegate) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return b.bucket.Attrs(ctx)
}

// NewWriter returns a storage Writer that writes to the GCS object
// associated with this ObjectHandle.
//
// A new object will be created unless an object with this name already exists.
// Otherwise any previous object with the same name will be replaced.
// The object will not be available (and any previous object will remain)
// until Close has been called.
//
// Attributes can be set on the object by modifying the returned Writer's
// ObjectAttrs field before the first call to Write. If no ContentType
// attribute is specified, the content type will be automatically sniffed
// using net/http.DetectContentType.
//
// It is the caller's responsibility to call Close when writing is done. To
// stop writing without saving the data, cancel the context.
func (o *objectDelegate) NewWriter(ctx context.Context) WriterWrapper {
	return &writerDelegate{writer: o.object.NewWriter(ctx)}
}

// NewReader creates a new Reader to read the contents of the
// object.
// ErrObjectNotExist will be returned if the object is not found.
//
// The caller must call Close on the returned Reader when done reading.
func (o *objectDelegate) NewReader(ctx context.Context) (ReaderWrapper, error) {
	r, err := o.object.NewReader(ctx)
	return &readerDelegate{reader: r}, err
}

func (o *objectDelegate) Attrs(ctx context.Context) (attrs *storage.ObjectAttrs, err error) {
	return o.object.Attrs(ctx)
}

// Close completes the write operation and flushes any buffered data.
// If Close doesn't return an error, metadata about the written object
// can be retrieved by calling Attrs.
func (w *writerDelegate) Close() error {
	return w.writer.Close()
}

// Write appends to w. It implements the io.Writer interface.
//
// Since writes happen asynchronously, Write may return a nil
// error even though the write failed (or will fail). Always
// use the error returned from Writer.Close to determine if
// the upload was successful.
func (w *writerDelegate) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

// CloseWithError aborts the write operation with the provided error.
// CloseWithError always returns nil.
//
// Deprecated: cancel the context passed to NewWriter instead.
func (w *writerDelegate) CloseWithError(err error) error {
	return w.writer.CloseWithError(err)
}

// Close closes the Reader. It must be called when done reading.
func (r *readerDelegate) Close() error {
	return r.reader.Close()
}

func (r *readerDelegate) Read(p []byte) (int, error) {
	return r.reader.Read(p)

}

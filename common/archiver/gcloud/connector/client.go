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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

var (
	// ErrBucketNotFound is non retryable error that is thrown when the bucket doesn't exist
	ErrBucketNotFound = errors.New("bucket not found")
	errObjectNotFound = errors.New("object not found")
)

type (
	// Precondition is a function that allow you to filter a query result.
	// If subject match params conditions then return true, else return false.
	Precondition func(subject interface{}) bool

	// Client is a wrapper around Google cloud storages client library.
	Client interface {
		Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error
		Get(ctx context.Context, URI archiver.URI, file string) ([]byte, error)
		Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error)
		QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error)
		Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error)
	}

	storageWrapper struct {
		client GcloudStorageClient
	}
)

// NewClient return a Temporal gcloudstorage.Client based on default google service account creadentials (ScopeFullControl required).
// Bucket must be created by Iaas scripts, in other words, this library doesn't create the required Bucket.
// Optionaly you can set your credential path throught "GOOGLE_APPLICATION_CREDENTIALS" environment variable or through temporal config file.
// You can find more info about "Google Setting Up Authentication for Server to Server Production Applications" under the following link
// https://cloud.google.com/docs/authentication/production
func NewClient(ctx context.Context, config *config.GstorageArchiver) (Client, error) {
	if credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); credentialsPath != "" {
		clientDelegate, err := newClientDelegateWithCredentials(ctx, credentialsPath)
		return &storageWrapper{client: clientDelegate}, err
	}

	if config.CredentialsPath != "" {
		clientDelegate, err := newClientDelegateWithCredentials(ctx, config.CredentialsPath)
		return &storageWrapper{client: clientDelegate}, err
	}

	clientDelegate, err := newDefaultClientDelegate(ctx)
	return &storageWrapper{client: clientDelegate}, err

}

// NewClientWithParams return a gcloudstorage.Client based on input parameters
func NewClientWithParams(clientD GcloudStorageClient) (Client, error) {
	return &storageWrapper{client: clientD}, nil
}

// Upload push a file to gcloud storage bucket (sinkPath)
// example:
// Upload(ctx, mockBucketHandleClient, "gs://my-bucket-cad/temporal_archival/development", "45273645-fileName.history", fileReader)
func (s *storageWrapper) Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) (err error) {
	bucket := s.client.Bucket(URI.Hostname())
	writer := bucket.Object(formatSinkPath(URI.Path()) + "/" + fileName).NewWriter(ctx)
	_, err = io.Copy(writer, bytes.NewReader(file))
	if err == nil {
		err = writer.Close()
	}

	return err
}

// Exist check if a bucket or an object exist
// If fileName is empty, then 'Exist' function will only check if the given bucket exist.
func (s *storageWrapper) Exist(ctx context.Context, URI archiver.URI, fileName string) (exists bool, err error) {
	bucket := s.client.Bucket(URI.Hostname())
	if _, err := bucket.Attrs(ctx); err != nil {
		return false, err
	}

	if fileName == "" {
		return true, nil
	}

	if _, err = bucket.Object(fileName).Attrs(ctx); err != nil {
		return false, errObjectNotFound
	}

	return true, nil
}

// Get retrieve a file
func (s *storageWrapper) Get(ctx context.Context, URI archiver.URI, fileName string) ([]byte, error) {
	bucket := s.client.Bucket(URI.Hostname())
	reader, err := bucket.Object(formatSinkPath(URI.Path()) + "/" + fileName).NewReader(ctx)
	if err == nil {
		defer reader.Close()
		return io.ReadAll(reader)
	}

	return nil, err
}

// Query, retieves file names by provided storage query
func (s *storageWrapper) Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) (fileNames []string, err error) {
	fileNames = make([]string, 0)
	bucket := s.client.Bucket(URI.Hostname())
	var attrs = new(storage.ObjectAttrs)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: formatSinkPath(URI.Path()) + "/" + fileNamePrefix,
	})

	for {
		attrs, err = it.Next()
		if err == iterator.Done {
			return fileNames, nil
		}
		fileNames = append(fileNames, attrs.Name)
	}

}

// QueryWithFilter, retieves filenames that match filter parameters. PageSize is optional, 0 means all records.
func (s *storageWrapper) QueryWithFilters(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize, offset int, filters []Precondition) ([]string, bool, int, error) {
	var err error
	currentPos := offset
	resultSet := make([]string, 0)
	bucket := s.client.Bucket(URI.Hostname())
	var attrs = new(storage.ObjectAttrs)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: formatSinkPath(URI.Path()) + "/" + fileNamePrefix,
	})

	for {
		attrs, err = it.Next()
		if err == iterator.Done {
			return resultSet, true, currentPos, nil
		}

		if completed := isPageCompleted(pageSize, len(resultSet)); completed {
			return resultSet, completed, currentPos, err
		}

		valid := true
		for _, f := range filters {
			if valid = f(attrs.Name); !valid {
				break
			}
		}

		if valid {
			if offset > 0 {
				offset--
				continue
			}
			// if match parsedQuery criteria and current cursor position is the last known position (offset is zero), append fileName to resultSet
			resultSet = append(resultSet, attrs.Name)
			currentPos++
		}
	}

}

func isPageCompleted(pageSize, currentPosition int) bool {
	return pageSize != 0 && currentPosition > 0 && pageSize <= currentPosition
}

func formatSinkPath(sinkPath string) string {
	return sinkPath[1:]
}

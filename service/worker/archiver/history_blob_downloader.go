// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"context"
	"encoding/json"
	"errors"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
)

type (
	// DownloadBlobRequest is request to DownloadBlob
	DownloadBlobRequest struct {
		NextPageToken        []byte
		ArchivalBucket       string
		DomainID             string
		WorkflowID           string
		RunID                string
		CloseFailoverVersion *int64
	}

	// DownloadBlobResponse is response from DownloadBlob
	DownloadBlobResponse struct {
		NextPageToken []byte
		HistoryBlob   *HistoryBlob
	}

	// HistoryBlobDownloader is used to download history blobs
	HistoryBlobDownloader interface {
		DownloadBlob(context.Context, *DownloadBlobRequest) (*DownloadBlobResponse, error)
	}

	historyBlobDownloader struct {
		blobstoreClient blobstore.Client
	}

	archivalToken struct {
		BlobstorePageToken   int
		CloseFailoverVersion int64
	}
)

var (
	errArchivalTokenCorrupted = &gen.BadRequestError{Message: "Next page token is corrupted."}
	errHistoryNotExist        = &gen.BadRequestError{Message: "Requested workflow history does not exist."}
)

// NewHistoryBlobDownloader returns a new HistoryBlobDownloader
func NewHistoryBlobDownloader(blobstoreClient blobstore.Client) HistoryBlobDownloader {
	return &historyBlobDownloader{
		blobstoreClient: blobstoreClient,
	}
}

// DownloadBlob is used to access a history blob from blobstore.
// CloseFailoverVersion can be optionally provided to get a specific version of a blob, if not provided gets highest available version.
func (d *historyBlobDownloader) DownloadBlob(ctx context.Context, request *DownloadBlobRequest) (*DownloadBlobResponse, error) {
	var token *archivalToken
	var err error
	if request.NextPageToken != nil {
		token, err = deserializeArchivalToken(request.NextPageToken)
		if err != nil {
			return nil, errArchivalTokenCorrupted
		}
	} else if request.CloseFailoverVersion != nil {
		token = &archivalToken{
			BlobstorePageToken:   common.FirstBlobPageToken,
			CloseFailoverVersion: *request.CloseFailoverVersion,
		}
	} else {
		indexKey, err := NewHistoryIndexBlobKey(request.DomainID, request.WorkflowID, request.RunID)
		if err != nil {
			return nil, err
		}
		indexTags, err := d.blobstoreClient.GetTags(ctx, request.ArchivalBucket, indexKey)
		if err == blobstore.ErrBlobNotExists {
			return nil, errHistoryNotExist
		} else if err != nil {
			return nil, err
		}
		highestVersion, err := GetHighestVersion(indexTags)
		if err != nil {
			return nil, err
		}
		token = &archivalToken{
			BlobstorePageToken:   common.FirstBlobPageToken,
			CloseFailoverVersion: *highestVersion,
		}
	}
	key, err := NewHistoryBlobKey(request.DomainID, request.WorkflowID, request.RunID, token.CloseFailoverVersion, token.BlobstorePageToken)
	if err != nil {
		return nil, err
	}
	b, err := d.blobstoreClient.Download(ctx, request.ArchivalBucket, key)
	if err == blobstore.ErrBlobNotExists {
		return nil, errHistoryNotExist
	} else if err != nil {
		return nil, err
	}
	unwrappedBlob, wrappingLayers, err := blob.Unwrap(b)
	if err != nil {
		return nil, err
	}
	historyBlob := &HistoryBlob{}
	switch *wrappingLayers.EncodingFormat {
	case blob.JSONEncoding:
		if err := json.Unmarshal(unwrappedBlob.Body, historyBlob); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown blob encoding format")
	}
	if *historyBlob.Header.IsLast {
		token = nil
	} else {
		token.BlobstorePageToken = *historyBlob.Header.NextPageToken
	}
	nextToken, err := serializeArchivalToken(token)
	if err != nil {
		return nil, err
	}
	return &DownloadBlobResponse{
		NextPageToken: nextToken,
		HistoryBlob:   historyBlob,
	}, nil
}

func deserializeArchivalToken(bytes []byte) (*archivalToken, error) {
	token := &archivalToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func serializeArchivalToken(token *archivalToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

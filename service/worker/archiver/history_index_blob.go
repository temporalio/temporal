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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/uber/cadence/common/blobstore/blob"
)

var (
	errNoKnownVersions = errors.New("history index blob contains no versions")
)

// NewHistoryIndexBlobKey returns a key for history index blob
func NewHistoryIndexBlobKey(domainID, workflowID, runID string) (blob.Key, error) {
	if len(domainID) == 0 || len(workflowID) == 0 || len(runID) == 0 {
		return nil, errInvalidKeyInput
	}
	domainIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(domainID)))
	workflowIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(workflowID)))
	runIDHash := fmt.Sprintf("%v", farm.Fingerprint64([]byte(runID)))
	combinedHash := strings.Join([]string{domainIDHash, workflowIDHash, runIDHash}, "")
	return blob.NewKey("index", combinedHash)
}

// GetHighestVersion returns the highest version from index blob tags
func GetHighestVersion(tags map[string]string) (*int64, error) {
	if tags == nil {
		return nil, errNoKnownVersions
	}
	var result *int64
	for tag := range tags {
		version, err := strconv.ParseInt(tag, 10, 64)
		if err != nil {
			continue
		}
		if result == nil || version > *result {
			result = &version
		}
	}
	if result == nil {
		return nil, errNoKnownVersions
	}
	return result, nil
}

func addVersion(closeFailoverVersion int64, existingVersions map[string]string) *blob.Blob {
	newVersion := strconv.FormatInt(closeFailoverVersion, 10)
	if existingVersions == nil {
		return blob.NewBlob(nil, map[string]string{newVersion: ""})
	}
	if _, ok := existingVersions[newVersion]; ok {
		return nil
	}
	newVersions := make(map[string]string)
	for ev := range existingVersions {
		newVersions[ev] = ""
	}
	newVersions[newVersion] = ""
	return blob.NewBlob(nil, newVersions)
}

func deleteVersion(closeFailoverVersion int64, existingVersions map[string]string) *blob.Blob {
	if existingVersions == nil {
		return nil
	}
	versionToDelete := strconv.FormatInt(closeFailoverVersion, 10)
	if _, ok := existingVersions[versionToDelete]; !ok {
		return nil
	}

	newVersions := make(map[string]string)
	for ev := range existingVersions {
		if ev != versionToDelete {
			newVersions[ev] = ""
		}
	}

	return blob.NewBlob(nil, newVersions)
}

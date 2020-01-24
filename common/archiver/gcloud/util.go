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

package gcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/.gen/go/shared"
)

func encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func decodeHistoryBatches(data []byte) ([]*shared.History, error) {
	historyBatches := []*shared.History{}
	err := json.Unmarshal(data, &historyBatches)
	if err != nil {
		return nil, err
	}
	return historyBatches, nil
}

func constructHistoryFilename(domainID, workflowID, runID string, version int64) string {
	combinedHash := constructHistoryFilenamePrefix(domainID, workflowID, runID)
	return fmt.Sprintf("%s_%v.history", combinedHash, version)
}

func constructHistoryFilenameMultipart(domainID, workflowID, runID string, version int64, partNumber int) string {
	combinedHash := constructHistoryFilenamePrefix(domainID, workflowID, runID)
	return fmt.Sprintf("%s_%v_%v.history", combinedHash, version, partNumber)
}

func constructHistoryFilenamePrefix(domainID, workflowID, runID string) string {
	return strings.Join([]string{hash(domainID), hash(workflowID), hash(runID)}, "")
}

func hash(s string) string {
	return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
}

func contextExpired(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func extractCloseFailoverVersion(filename string) (int64, int, error) {
	filenameParts := strings.FieldsFunc(filename, func(r rune) bool {
		return r == '_' || r == '.'
	})
	if len(filenameParts) != 4 {
		return -1, 0, errors.New("unknown filename structure")
	}

	failoverVersion, err := strconv.ParseInt(filenameParts[1], 10, 64)
	if err != nil {
		return -1, 0, err
	}

	highestPart, err := strconv.Atoi(filenameParts[2])
	return failoverVersion, highestPart, err
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

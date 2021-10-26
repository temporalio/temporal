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

package persistence

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	queryDelimiter        = ";"
	querySliceDefaultSize = 100
)

// LoadAndSplitQuery loads and split cql / sql query into one statement per string
func LoadAndSplitQuery(
	filePaths []string,
) ([]string, error) {
	var files []io.Reader

	for _, filePath := range filePaths {
		f, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("error opening file %s: %w", filePath, err)
		}
		files = append(files, f)
	}

	return LoadAndSplitQueryFromReaders(files)
}

// LoadAndSplitQueryFromReaders loads and split cql / sql query into one statement per string
func LoadAndSplitQueryFromReaders(
	readers []io.Reader,
) ([]string, error) {

	result := make([]string, 0, querySliceDefaultSize)

	for _, r := range readers {
		content, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("error reading contents: %w", err)
		}
		for _, stmt := range strings.Split(string(content), queryDelimiter) {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			result = append(result, stmt)
		}

	}
	return result, nil
}

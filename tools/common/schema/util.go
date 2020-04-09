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

package schema

import (
	"bufio"
	"io"
	"os"
	"strings"
)

const newLineDelim = '\n'

// ParseFile takes a cql / sql file path as input
// and returns an array of cql / sql statements on
// success.
func ParseFile(filePath string) ([]string, error) {
	// #nosec
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)

	var line string
	var currStmt string
	var stmts = make([]string, 0, 4)

	for err == nil {

		line, err = reader.ReadString(newLineDelim)
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue
		}

		// Filter out the comment lines, the
		// only recognized comment line format
		// is any line that starts with double dashes
		tokens := strings.Split(line, "--")
		if len(tokens) > 0 && len(tokens[0]) > 0 {
			currStmt += tokens[0]
			// semi-colon is the end of statement delim
			if strings.HasSuffix(currStmt, ";") {
				stmts = append(stmts, currStmt)
				currStmt = ""
			}
		}
	}

	if err == io.EOF {
		return stmts, nil
	}

	return nil, err
}

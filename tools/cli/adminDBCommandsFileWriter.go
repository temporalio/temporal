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

package cli

import (
	"encoding/json"
	"os"
	"strings"
)

const (
	flushThreshold = 50
)

type (
	// BufferedWriter is used to buffer entities and write them to a file
	BufferedWriter interface {
		Add(interface{})
		Flush()
	}

	bufferedWriter struct {
		f       *os.File
		entries []interface{}
	}
)

// NewBufferedWriter constructs a new BufferedWriter
func NewBufferedWriter(f *os.File) BufferedWriter {
	return &bufferedWriter{
		f: f,
	}
}

// Add adds a new entity
func (bw *bufferedWriter) Add(e interface{}) {
	if len(bw.entries) > flushThreshold {
		bw.Flush()
	}
	bw.entries = append(bw.entries, e)
}

// Flush flushes contents to file
func (bw *bufferedWriter) Flush() {
	var builder strings.Builder
	for _, e := range bw.entries {
		if err := bw.writeToBuilder(&builder, e); err != nil {
			ErrorAndExit("failed to write to builder", err)
		}
	}
	if err := bw.writeBuilderToFile(&builder, bw.f); err != nil {
		ErrorAndExit("failed to write to file", err)
	}
	bw.entries = nil
}

func (bw *bufferedWriter) writeToBuilder(builder *strings.Builder, e interface{}) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	builder.WriteString(string(data))
	builder.WriteString("\r\n")
	return nil
}

func (bw *bufferedWriter) writeBuilderToFile(builder *strings.Builder, f *os.File) error {
	_, err := f.WriteString(builder.String())
	return err
}

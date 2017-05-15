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

package common

const (
	// FirstEventID is the id of the first event in the history
	FirstEventID int64 = 1
	// EmptyEventID is the id of the empty event
	EmptyEventID int64 = -23
)

const (
	// FrontendServiceName is the name of the frontend service
	FrontendServiceName = "cadence-frontend"
	// HistoryServiceName is the name of the history service
	HistoryServiceName = "cadence-history"
	// MatchingServiceName is the name of the matching service
	MatchingServiceName = "cadence-matching"
)

// Data encoding types
const (
	EncodingTypeJSON EncodingType = "json"
	EncodingTypeGob               = "gob"
)

type (
	// EncodingType is an enum that represents various data encoding types
	EncodingType string
)
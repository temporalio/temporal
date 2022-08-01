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

package gocql

import (
	"fmt"

	"github.com/gocql/gocql"
)

// Definition of all Consistency levels
const (
	Any Consistency = iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	LocalOne
)

// Definition of all SerialConsistency levels
const (
	Serial SerialConsistency = iota
	LocalSerial
)

func mustConvertConsistency(c Consistency) gocql.Consistency {
	switch c {
	case Any:
		return gocql.Any
	case One:
		return gocql.One
	case Two:
		return gocql.Two
	case Three:
		return gocql.Three
	case Quorum:
		return gocql.Quorum
	case All:
		return gocql.All
	case LocalQuorum:
		return gocql.LocalQuorum
	case EachQuorum:
		return gocql.EachQuorum
	case LocalOne:
		return gocql.LocalOne
	default:
		panic(fmt.Sprintf("Unknown gocql Consistency level: %v", c))
	}
}

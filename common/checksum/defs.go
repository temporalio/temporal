// Copyright (c) 2019 Uber Technologies, Inc.
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

package checksum

import "errors"

type (
	// Checksum represents a checksum value along
	// with associated metadata
	Checksum struct {
		// Version represents version of the payload from
		Version int
		// which this checksum was derived
		Flavor Flavor
		// Value is the checksum value
		Value []byte
	}

	// Flavor is an enum type that represents the type of checksum
	Flavor int
)

const (
	// FlavorUnknown represents an unknown/uninitialized checksum flavor
	FlavorUnknown Flavor = iota
	// FlavorIEEECRC32OverThriftBinary represents crc32 checksum generated over thriftRW serialized payload
	FlavorIEEECRC32OverThriftBinary
	maxFlavors
)

// ErrMismatch indicates a checksum verification failure due to
// a derived checksum not being equal to expected checksum
var ErrMismatch = errors.New("checksum mismatch error")

// IsValid returns true if the checksum flavor is valid
func (f Flavor) IsValid() bool {
	return f > FlavorUnknown && f < maxFlavors
}

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

package membership

// HostInfo represents the host of a Temporal service.
type HostInfo interface {
	// Identity returns the unique identifier of the host.
	// This may be the same as the address.
	Identity() string
	// GetAddress returns the socket address of the host (i.e. <ip>:<port>).
	// This must be a valid gRPC address.
	GetAddress() string
}

// NewHostInfoFromAddress creates a new HostInfo instance from a socket address.
func NewHostInfoFromAddress(address string) HostInfo {
	return hostAddress(address)
}

// hostAddress is a HostInfo implementation that uses a string as the address and identity.
type hostAddress string

// GetAddress returns the value of the hostAddress.
func (a hostAddress) GetAddress() string {
	return string(a)
}

// Identity returns the value of the hostAddress.
func (a hostAddress) Identity() string {
	return string(a)
}

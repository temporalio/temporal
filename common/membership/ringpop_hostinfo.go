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

type ringpopHostInfo struct {
	addr   string // ip:port
	labels map[string]string
}

// newRingpopHostInfo creates a new *ringpopHostInfo instance
func newRingpopHostInfo(addr string, labels map[string]string) *ringpopHostInfo {
	if labels == nil {
		labels = make(map[string]string)
	}
	return &ringpopHostInfo{
		addr:   addr,
		labels: labels,
	}
}

// GetAddress returns the ip:port address
func (hi *ringpopHostInfo) GetAddress() string {
	return hi.addr
}

// Identity implements ringpop's Membership interface
func (hi *ringpopHostInfo) Identity() string {
	// for now we just use the address as the identity
	return hi.addr
}

// Label implements ringpop's Membership interface
func (hi *ringpopHostInfo) Label(key string) (value string, has bool) {
	value, has = hi.labels[key]
	return
}

// SetLabel sets the label.
func (hi *ringpopHostInfo) SetLabel(key string, value string) {
	hi.labels[key] = value
}

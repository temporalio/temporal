// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

//go:build utest

package namespace

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func NewStubNamespace() *Namespace {
	return &Namespace{
		replicationConfig: &persistencespb.NamespaceReplicationConfig{},
	}
}

type StubRegistry struct {
	registry
	NS *Namespace
}

func (sr *StubRegistry) GetNamespace(_ Name) (*Namespace, error) {
	return sr.NS, nil
}
func (sr *StubRegistry) GetNamespaceByID(_ ID) (*Namespace, error) {
	return sr.NS, nil
}
func (sr *StubRegistry) GetNamespaceID(_ Name) (ID, error) {
	return sr.NS.ID(), nil
}
func (sr *StubRegistry) GetNamespaceName(_ ID) (Name, error) {
	return sr.NS.Name(), nil
}
func (sr *StubRegistry) GetCacheSize() (int64, int64) {
	return 0, 0
}

func (sr *StubRegistry) GetCustomSearchAttributesMapper(_ Name) (CustomSearchAttributesMapper, error) {
	return CustomSearchAttributesMapper{}, nil
}

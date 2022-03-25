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

package workflow

import (
	"go.temporal.io/server/service/history/shard"
)

var (
	// The default value is just for testing purpose,
	// so we don't have to specify its value in every test.
	// If TaskGeneratorFactory is not provided as an fx Option,
	// fx.Populate in fx.go and server start up will still fail.
	taskGeneratorProvider = NewTaskGeneratorProvider()
)

type (
	TaskGeneratorProvider interface {
		NewTaskGenerator(shard.Context, MutableState) TaskGenerator
	}

	taskGeneratorProviderImpl struct{}
)

func NewTaskGeneratorProvider() TaskGeneratorProvider {
	return &taskGeneratorProviderImpl{}
}

func (p *taskGeneratorProviderImpl) NewTaskGenerator(
	shard shard.Context,
	mutableState MutableState,
) TaskGenerator {
	return NewTaskGenerator(
		shard.GetNamespaceRegistry(),
		mutableState,
	)
}

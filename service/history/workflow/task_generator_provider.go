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
	// This is set as a global to avoid plumbing through many layers. The default
	// implementation has no state so this is safe even though it bypasses DI.
	// It's set to a default value statically so that we can avoid setting it in tests, which
	// would trip the race detector when running multiple servers in a single process.
	// Note that TaskGeneratorProvider still needs to be provided through fx even with this
	// default value.
	taskGeneratorProvider TaskGeneratorProvider = defaultTaskGeneratorProvider

	defaultTaskGeneratorProvider TaskGeneratorProvider = &taskGeneratorProviderImpl{}
)

type (
	TaskGeneratorProvider interface {
		NewTaskGenerator(shard.Context, MutableState) TaskGenerator
	}

	taskGeneratorProviderImpl struct{}
)

func populateTaskGeneratorProvider(provider TaskGeneratorProvider) {
	if provider == defaultTaskGeneratorProvider {
		return // avoid setting default over default to eliminate racey writes during testing
	}
	taskGeneratorProvider = provider
}

func GetTaskGeneratorProvider() TaskGeneratorProvider {
	return taskGeneratorProvider
}

func (p *taskGeneratorProviderImpl) NewTaskGenerator(
	shard shard.Context,
	mutableState MutableState,
) TaskGenerator {
	return NewTaskGenerator(
		shard.GetNamespaceRegistry(),
		mutableState,
		shard.GetConfig(),
		shard.GetArchivalMetadata(),
	)
}

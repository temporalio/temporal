package workflow

import (
	"sync/atomic"

	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	TaskGeneratorProvider interface {
		NewTaskGenerator(historyi.ShardContext, historyi.MutableState) TaskGenerator
	}
	taskGeneratorProviderImpl struct{}
)

var (
	// This is set as a global to avoid plumbing through many layers. The default
	// implementation has no state so this is safe even though it bypasses DI.
	// It's set to a default value in the init func so that we can avoid setting it in tests.
	// Note that TaskGeneratorProvider still needs to be provided through fx even with this
	// default value.
	_taskGeneratorProvider atomic.Pointer[TaskGeneratorProvider]
)

func init() {
	var defaultProvider TaskGeneratorProvider = new(taskGeneratorProviderImpl)
	populateTaskGeneratorProvider(defaultProvider)
}

func populateTaskGeneratorProvider(provider TaskGeneratorProvider) {
	_taskGeneratorProvider.Store(&provider)
}

func GetTaskGeneratorProvider() TaskGeneratorProvider {
	return *_taskGeneratorProvider.Load()
}

func (p *taskGeneratorProviderImpl) NewTaskGenerator(
	shard historyi.ShardContext,
	mutableState historyi.MutableState,
) TaskGenerator {
	return NewTaskGenerator(
		shard.GetNamespaceRegistry(),
		mutableState,
		shard.GetConfig(),
		shard.GetArchivalMetadata(),
		shard.GetLogger(),
	)
}

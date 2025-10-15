package workflow

import (
	historyi "go.temporal.io/server/service/history/interfaces"
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
		NewTaskGenerator(historyi.ShardContext, historyi.MutableState) TaskGenerator
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

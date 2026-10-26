package workerdeployment

import (
	"github.com/bits-and-blooms/bloom/v3"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
)

const taskQueueFamilyBloomFalsePositiveRate = 0.01

func buildTaskQueueFamilySummary(
	taskQueueFamilies map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData,
) *deploymentspb.TaskQueueFamilySummary {
	summary := &deploymentspb.TaskQueueFamilySummary{Count: int32(len(taskQueueFamilies))}
	if len(taskQueueFamilies) == 0 {
		return summary
	}

	filter := bloom.NewWithEstimates(uint(len(taskQueueFamilies)), taskQueueFamilyBloomFalsePositiveRate)
	for _, taskQueueName := range workflow.DeterministicKeys(taskQueueFamilies) {
		filter.AddString(taskQueueName)
	}

	summary.BloomFilterSize = int64(filter.Cap())
	summary.BloomFilterHashCount = int32(filter.K())
	summary.BloomFilterWords = make([]int64, len(filter.BitSet().Words()))
	for index, word := range filter.BitSet().Words() {
		summary.BloomFilterWords[index] = int64(word)
	}
	return summary
}

func taskQueueFamilyMayExist(summary *deploymentspb.TaskQueueFamilySummary, taskQueueName string) bool {
	count := summary.GetCount()
	if count <= 0 {
		return true
	}

	filterWords := summary.GetBloomFilterWords()
	// The proto uses int64 words to satisfy proto lint, while the Bloom library requires uint64.
	// This conversion preserves the existing bitset; it does not rebuild the filter from task queue names.
	words := make([]uint64, len(filterWords))
	for index, word := range filterWords {
		words[index] = uint64(word)
	}
	return bloom.FromWithM(
		words,
		uint(summary.GetBloomFilterSize()),
		uint(summary.GetBloomFilterHashCount()),
	).TestString(taskQueueName)
}

func isTaskQueueFamilySummaryValid(summary *deploymentspb.TaskQueueFamilySummary) bool {
	count := summary.GetCount()
	if count <= 0 {
		return false
	}
	expectedSize, expectedHashCount := bloom.EstimateParameters(uint(count), taskQueueFamilyBloomFalsePositiveRate)

	// validations to ensure that the Bloom filter is valid
	return summary.GetBloomFilterSize() == int64(expectedSize) &&
		summary.GetBloomFilterHashCount() == int32(expectedHashCount) &&
		uint(len(summary.GetBloomFilterWords())) == (expectedSize+63)/64
}

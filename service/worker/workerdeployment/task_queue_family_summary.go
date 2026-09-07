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

	serializedFilter, err := filter.MarshalBinary()
	if err != nil {
		return nil
	}
	summary.BloomFilter = serializedFilter
	return summary
}

func taskQueueFamilyMayExist(summary *deploymentspb.TaskQueueFamilySummary, taskQueueName string) bool {
	serializedFilter := summary.GetBloomFilter()
	if len(serializedFilter) == 0 {
		return true
	}

	// TODO: Consider validating the Bloom filter's encoded metadata before decoding.
	var filter bloom.BloomFilter
	if err := filter.UnmarshalBinary(serializedFilter); err != nil {
		return true
	}
	return filter.TestString(taskQueueName)
}

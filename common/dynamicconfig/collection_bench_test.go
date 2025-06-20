package dynamicconfig_test

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func BenchmarkCollection(b *testing.B) {
	// client with just one value
	client1 := dynamicconfig.StaticClient{
		dynamicconfig.MatchingMaxTaskBatchSize.Key(): []dynamicconfig.ConstrainedValue{{Value: 12}},
	}
	cln1 := dynamicconfig.NewCollection(client1, log.NewNoopLogger())
	b.Run("global int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.MatchingThrottledLogRPS.Get(cln1)
			_ = size()
			size = dynamicconfig.MatchingThrottledLogRPS.Get(cln1)
			_ = size()
		}
	})
	b.Run("namespace int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.HistoryMaxPageSize.Get(cln1)
			_ = size("my-namespace")
			size = dynamicconfig.WorkflowExecutionMaxInFlightUpdates.Get(cln1)
			_ = size("my-namespace")
		}
	})
	b.Run("taskqueue int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingGetTasksBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
		}
	})

	// client with more constrained values
	client2 := dynamicconfig.StaticClient{
		dynamicconfig.MatchingMaxTaskBatchSize.Key(): []dynamicconfig.ConstrainedValue{
			{
				Constraints: dynamicconfig.Constraints{
					TaskQueueName: "other-tq",
				},
				Value: 18,
			},
			{
				Constraints: dynamicconfig.Constraints{
					Namespace: "other-ns",
				},
				Value: 15,
			},
		},
	}
	cln2 := dynamicconfig.NewCollection(client2, log.NewNoopLogger())
	b.Run("single default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
	b.Run("structured default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
}

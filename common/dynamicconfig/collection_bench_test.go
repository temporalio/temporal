package dynamicconfig_test

import (
	"fmt"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func BenchmarkCollection(b *testing.B) {
	client1 := dynamicconfig.StaticClient{
		dynamicconfig.MatchingMaxTaskBatchSize.Key():      []dynamicconfig.ConstrainedValue{{Value: 12}},
		dynamicconfig.HistoryRPS.Key():                    []dynamicconfig.ConstrainedValue{{Value: 100}},
		dynamicconfig.BlobSizeLimitError.Key():            []dynamicconfig.ConstrainedValue{{Value: 100}},
		dynamicconfig.BlobSizeLimitWarn.Key():             []dynamicconfig.ConstrainedValue{{Value: 100}},
		dynamicconfig.MatchingShutdownDrainDuration.Key(): []dynamicconfig.ConstrainedValue{{Value: "100s"}},
	}
	cln1 := dynamicconfig.NewCollection(client1, log.NewNoopLogger())
	b.Run("global int default", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingThrottledLogRPS.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size()
			_ = size()
		}
	})
	b.Run("global int present", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.HistoryRPS.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size()
			_ = size()
		}
	})
	b.Run("namespace int default", func(b *testing.B) {
		b.ReportAllocs()
		size1 := dynamicconfig.HistoryMaxPageSize.Get(cln1)
		size2 := dynamicconfig.WorkflowExecutionMaxInFlightUpdates.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size1("my-namespace")
			_ = size2("my-namespace")
		}
	})
	b.Run("namespace int present", func(b *testing.B) {
		b.ReportAllocs()
		size1 := dynamicconfig.BlobSizeLimitError.Get(cln1)
		size2 := dynamicconfig.BlobSizeLimitWarn.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size1("my-namespace")
			_ = size2("my-namespace")
		}
	})
	b.Run("taskqueue int default", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingMaxTaskDeleteBatchSize.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size("my-namespace", "my-task-queue", 1)
			_ = size("my-namespace", "my-task-queue", 1)
		}
	})
	b.Run("taskqueue int present", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size("my-namespace", "my-task-queue", 1)
			_ = size("my-namespace", "my-task-queue", 1)
		}
	})
	b.Run("global duration default", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingAlignMembershipChange.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size()
			_ = size()
		}
	})
	b.Run("global duration present", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingShutdownDrainDuration.Get(cln1)
		for i := 0; i < b.N/2; i++ {
			_ = size()
			_ = size()
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
		size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
		for i := 0; i < b.N/4; i++ {
			_ = size("my-namespace", "my-task-queue", 1)
			_ = size("my-namespace", "other-tq", 1)
			_ = size("other-ns", "my-task-queue", 1)
			_ = size("other-ns", "other-tq", 1)
		}
	})
	b.Run("structured default", func(b *testing.B) {
		b.ReportAllocs()
		size := dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
		for i := 0; i < b.N/4; i++ {
			_ = size("my-namespace", "my-task-queue", 1)
			_ = size("my-namespace", "other-tq", 1)
			_ = size("other-ns", "my-task-queue", 1)
			_ = size("other-ns", "other-tq", 1)
		}
	})
}

func BenchmarkCollectionIndexed(b *testing.B) {
	// You might want to set constraintsCacheThreshold to a high value before running this to
	// measure the performance of linear search.

	var nums []int
	for v := 1.0; v < 1000; v *= 1.5 {
		nums = append(nums, int(v+0.999))
	}
	for _, numNs := range nums {
		// query for the middle one to measure the average
		queryNs := numNs / 2

		b.Run(fmt.Sprintf("num%d", numNs), func(b *testing.B) {
			cvs := make([]dynamicconfig.ConstrainedValue, numNs)
			for i := range cvs {
				cvs[i] = dynamicconfig.ConstrainedValue{
					Constraints: dynamicconfig.Constraints{
						Namespace: fmt.Sprintf("namespace%d", i),
					},
					Value: 1000 + i,
				}
			}

			cli := dynamicconfig.StaticClient{
				dynamicconfig.FrontendGlobalNamespaceRPS.Key(): cvs,
			}
			cln := dynamicconfig.NewCollection(cli, log.NewNoopLogger())
			get := dynamicconfig.FrontendGlobalNamespaceRPS.Get(cln)
			query := fmt.Sprintf("namespace%d", queryNs)

			for b.Loop() {
				get(query)
			}
		})
	}
}

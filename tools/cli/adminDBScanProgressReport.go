// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cli

import (
	"math"
	"time"
)

type (
	// ShardScanReport is the type that gets written to ShardScanReportFile
	ShardScanReport struct {
		ShardID         int
		TotalDBRequests int64
		Scanned         *ShardScanReportExecutionsScanned
		Failure         *ShardScanReportFailure
	}

	// ShardScanReportExecutionsScanned is the part of the ShardScanReport of executions which were scanned
	ShardScanReportExecutionsScanned struct {
		TotalExecutionsCount       int64
		CorruptedExecutionsCount   int64
		ExecutionCheckFailureCount int64
		CorruptionTypeBreakdown    CorruptionTypeBreakdown
		OpenCorruptions            OpenCorruptions
	}

	// ShardScanReportFailure is the part of the ShardScanReport that indicates failure to scan all or part of the shard
	ShardScanReportFailure struct {
		Note    string
		Details string
	}

	// ProgressReport contains metadata about the scan for all shards which have been finished
	// This is periodically printed to stdout
	ProgressReport struct {
		ShardStats     ShardStats
		ExecutionStats ExecutionStats
		Rates          Rates
	}

	// ShardStats breaks out shard level stats
	ShardStats struct {
		NumberOfShardsFinished    int
		NumberOfShardScanFailures int
		ShardsFailed              []int
		MinExecutions             *int64
		MaxExecutions             *int64
		AverageExecutions         int64
	}

	// ExecutionStats breaks down execution level stats
	ExecutionStats struct {
		TotalExecutionsCount int64
		CorruptionStats      CorruptionStats
		CheckFailureStats    CheckFailureStats
	}

	// CorruptionStats breaks out stats regarding corrupted executions
	CorruptionStats struct {
		CorruptedExecutionsCount int64
		PercentageCorrupted      float64
		CorruptionTypeBreakdown  CorruptionTypeBreakdown
		OpenCorruptions          OpenCorruptions
	}

	// CheckFailureStats breaks out stats regarding execution check failures
	CheckFailureStats struct {
		ExecutionCheckFailureCount int64
		PercentageCheckFailure     float64
	}

	// CorruptionTypeBreakdown breaks down counts and percentages of corruption types
	CorruptionTypeBreakdown struct {
		TotalHistoryMissing                            int64
		TotalInvalidFirstEvent                         int64
		TotalOpenExecutionInvalidCurrentExecution      int64
		PercentageHistoryMissing                       float64
		PercentageInvalidStartEvent                    float64
		PercentageOpenExecutionInvalidCurrentExecution float64
	}

	// OpenCorruptions breaks down the count and percentage of open workflows which are corrupted
	OpenCorruptions struct {
		TotalOpen      int64
		PercentageOpen float64
	}

	// Rates indicates the rates at which the scan is progressing
	Rates struct {
		TimeRunning       string
		DatabaseRPS       float64
		TotalDBRequests   int64
		ShardsPerHour     float64
		ExecutionsPerHour float64
	}
)

func includeShardInProgressReport(report *ShardScanReport, progressReport *ProgressReport, startTime time.Time) {
	includeExecutionStats(report, &progressReport.ExecutionStats)
	includeShardStats(report, &progressReport.ShardStats, progressReport.ExecutionStats.TotalExecutionsCount)
	includeRates(report, &progressReport.Rates, startTime, progressReport.ExecutionStats.TotalExecutionsCount, progressReport.ShardStats.NumberOfShardsFinished)
}

func includeExecutionStats(report *ShardScanReport, executionStats *ExecutionStats) {
	if report.Scanned != nil {
		executionStats.TotalExecutionsCount += report.Scanned.TotalExecutionsCount
		executionStats.CorruptionStats.CorruptedExecutionsCount += report.Scanned.CorruptedExecutionsCount
		executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalHistoryMissing += report.Scanned.CorruptionTypeBreakdown.TotalHistoryMissing
		executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution += report.Scanned.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution
		executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalInvalidFirstEvent += report.Scanned.CorruptionTypeBreakdown.TotalInvalidFirstEvent
		executionStats.CorruptionStats.OpenCorruptions.TotalOpen += report.Scanned.OpenCorruptions.TotalOpen
		executionStats.CheckFailureStats.ExecutionCheckFailureCount += report.Scanned.ExecutionCheckFailureCount
	}
	if executionStats.TotalExecutionsCount > 0 {
		executionStats.CorruptionStats.PercentageCorrupted = math.Round((float64(executionStats.CorruptionStats.CorruptedExecutionsCount) * 100.0) /
			float64(executionStats.TotalExecutionsCount))

		executionStats.CheckFailureStats.PercentageCheckFailure = math.Round((float64(executionStats.CheckFailureStats.ExecutionCheckFailureCount) * 100.0) /
			float64(executionStats.TotalExecutionsCount))
	}

	if executionStats.CorruptionStats.CorruptedExecutionsCount > 0 {
		executionStats.CorruptionStats.CorruptionTypeBreakdown.PercentageHistoryMissing = math.Round((float64(executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalHistoryMissing) * 100.0) /
			float64(executionStats.CorruptionStats.CorruptedExecutionsCount))

		executionStats.CorruptionStats.CorruptionTypeBreakdown.PercentageInvalidStartEvent = math.Round((float64(executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalInvalidFirstEvent) * 100.0) /
			float64(executionStats.CorruptionStats.CorruptedExecutionsCount))

		executionStats.CorruptionStats.CorruptionTypeBreakdown.PercentageOpenExecutionInvalidCurrentExecution = math.Round((float64(executionStats.CorruptionStats.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution) * 100.0) /
			float64(executionStats.CorruptionStats.CorruptedExecutionsCount))

		executionStats.CorruptionStats.OpenCorruptions.PercentageOpen = math.Round((float64(executionStats.CorruptionStats.OpenCorruptions.TotalOpen) * 100.0) /
			float64(executionStats.CorruptionStats.CorruptedExecutionsCount))
	}
}

func includeShardStats(report *ShardScanReport, shardStats *ShardStats, totalExecutions int64) {
	shardStats.NumberOfShardsFinished++
	if report.Failure != nil {
		shardStats.NumberOfShardScanFailures++
		shardStats.ShardsFailed = append(shardStats.ShardsFailed, report.ShardID)
	}
	if report.Scanned != nil &&
		(shardStats.MinExecutions == nil || *shardStats.MinExecutions > report.Scanned.TotalExecutionsCount) {
		shardStats.MinExecutions = &report.Scanned.TotalExecutionsCount
	}
	if report.Scanned != nil &&
		(shardStats.MaxExecutions == nil || *shardStats.MaxExecutions < report.Scanned.TotalExecutionsCount) {
		shardStats.MaxExecutions = &report.Scanned.TotalExecutionsCount
	}
	successfullyFinishedShards := shardStats.NumberOfShardsFinished - shardStats.NumberOfShardScanFailures
	if successfullyFinishedShards > 0 {
		shardStats.AverageExecutions = totalExecutions / int64(successfullyFinishedShards)
	}
}

func includeRates(report *ShardScanReport, rates *Rates, startTime time.Time, totalExecutions int64, numberOfShardsFinished int) {
	rates.TotalDBRequests += report.TotalDBRequests
	rates.TimeRunning = time.Now().Sub(startTime).String()
	pastTime := time.Now().Sub(startTime)
	hoursPast := float64(pastTime) / float64(time.Hour)
	secondsPast := float64(pastTime) / float64(time.Second)
	rates.ShardsPerHour = math.Round(float64(numberOfShardsFinished) / hoursPast)
	rates.ExecutionsPerHour = math.Round(float64(totalExecutions) / hoursPast)
	rates.DatabaseRPS = math.Round(float64(rates.TotalDBRequests) / secondsPast)
}

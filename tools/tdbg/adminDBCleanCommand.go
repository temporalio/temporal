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

package tdbg

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	cassp "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/quotas"
)

type (
	// ShardCleanReport represents the result of cleaning a single shard
	ShardCleanReport struct {
		ShardID         int32
		TotalDBRequests int64
		Handled         *ShardCleanReportHandled
		Failure         *ShardCleanReportFailure
	}

	// ShardCleanReportHandled is the part of ShardCleanReport of executions which were read from corruption file
	// and were attempted to be deleted
	ShardCleanReportHandled struct {
		TotalExecutionsCount     int64
		SuccessfullyCleanedCount int64
		FailedCleanedCount       int64
	}

	// ShardCleanReportFailure is the part of ShardCleanReport that indicates a failure to clean some or all
	// of the executions found in corruption file
	ShardCleanReportFailure struct {
		Note    string
		Details string
	}

	// CleanProgressReport represents the aggregate progress of the clean job.
	// It is periodically printed to stdout
	CleanProgressReport struct {
		NumberOfShardsFinished     int
		TotalExecutionsCount       int64
		SuccessfullyCleanedCount   int64
		FailedCleanedCount         int64
		TotalDBRequests            int64
		DatabaseRPS                float64
		NumberOfShardCleanFailures int64
		ShardsPerHour              float64
		ExecutionsPerHour          float64
	}

	// CleanOutputDirectories are the directory paths for output of clean
	CleanOutputDirectories struct {
		ShardCleanReportDirectoryPath    string
		SuccessfullyCleanedDirectoryPath string
		FailedCleanedDirectoryPath       string
	}

	// ShardCleanOutputFiles are the files produced for a clean of a single shard
	ShardCleanOutputFiles struct {
		ShardCleanReportFile    *os.File
		SuccessfullyCleanedFile *os.File
		FailedCleanedFile       *os.File
	}
)

func cleanShard(
	ctx context.Context,
	limiter quotas.RateLimiter,
	session gocql.Session,
	outputDirectories *CleanOutputDirectories,
	inputDirectory string,
	shardID int32,
) (*ShardCleanReport, error) {
	outputFiles, closeFn, err := createShardCleanOutputFiles(shardID, outputDirectories)
	if err != nil {
		return nil, err
	}

	report := &ShardCleanReport{
		ShardID: shardID,
	}
	failedCleanWriter := NewBufferedWriter(outputFiles.FailedCleanedFile)
	successfullyCleanWriter := NewBufferedWriter(outputFiles.SuccessfullyCleanedFile)
	defer func() {
		failedCleanWriter.Flush()
		successfullyCleanWriter.Flush()
		recordShardCleanReport(outputFiles.ShardCleanReportFile, report)
		deleteEmptyFiles(outputFiles.ShardCleanReportFile, outputFiles.SuccessfullyCleanedFile, outputFiles.FailedCleanedFile)
		closeFn()
	}()
	shardCorruptedFile, err := getShardCorruptedFile(inputDirectory, shardID)
	if err != nil {
		if !os.IsNotExist(err) {
			report.Failure = &ShardCleanReportFailure{
				Note:    "failed to get corruption file",
				Details: err.Error(),
			}
		}
		return report, nil
	}
	defer shardCorruptedFile.Close()
	execStore := cassp.NewExecutionStore(session, log.NewNoopLogger())

	scanner := bufio.NewScanner(shardCorruptedFile)
	for scanner.Scan() {
		if report.Handled == nil {
			report.Handled = &ShardCleanReportHandled{}
		}
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		report.Handled.TotalExecutionsCount++
		var ce CorruptedExecution
		err := json.Unmarshal([]byte(line), &ce)
		if err != nil {
			report.Handled.FailedCleanedCount++
			continue
		}

		deleteConcreteReq := &persistence.DeleteWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: ce.NamespaceID,
			WorkflowID:  ce.WorkflowID,
			RunID:       ce.RunID,
		}
		preconditionForDBCall(&report.TotalDBRequests, limiter)
		err = execStore.DeleteWorkflowExecution(ctx, deleteConcreteReq)
		if err != nil {
			report.Handled.FailedCleanedCount++
			failedCleanWriter.Add(&ce)
			continue
		}
		report.Handled.SuccessfullyCleanedCount++
		successfullyCleanWriter.Add(&ce)
		if ce.CorruptedExceptionMetadata.CorruptionType != OpenExecutionInvalidCurrentExecution {
			deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
				ShardID:     shardID,
				NamespaceID: ce.NamespaceID,
				WorkflowID:  ce.WorkflowID,
				RunID:       ce.RunID,
			}
			// deleting current execution is best effort, the success or failure of the cleanup
			// is determined above based on if the concrete execution could be deleted
			preconditionForDBCall(&report.TotalDBRequests, limiter)
			execStore.DeleteCurrentWorkflowExecution(ctx, deleteCurrentReq)
		}
		// TODO: we will want to also cleanup history for corrupted workflows, this will be punted on until this is converted to a workflow
	}
	return report, nil
}

func getShardCorruptedFile(inputDir string, shardID int32) (*os.File, error) {
	filepath := fmt.Sprintf("%v/%v", inputDir, constructFileNameFromShard(shardID))
	return os.Open(filepath)
}

func includeShardCleanInProgressReport(report *ShardCleanReport, progressReport *CleanProgressReport, startTime time.Time) {
	progressReport.NumberOfShardsFinished++
	progressReport.TotalDBRequests += report.TotalDBRequests
	if report.Failure != nil {
		progressReport.NumberOfShardCleanFailures++
	}

	if report.Handled != nil {
		progressReport.TotalExecutionsCount += report.Handled.TotalExecutionsCount
		progressReport.FailedCleanedCount += report.Handled.FailedCleanedCount
		progressReport.SuccessfullyCleanedCount += report.Handled.SuccessfullyCleanedCount
	}

	pastTime := time.Now().UTC().Sub(startTime)
	hoursPast := float64(pastTime) / float64(time.Hour)
	progressReport.ShardsPerHour = math.Round(float64(progressReport.NumberOfShardsFinished) / hoursPast)
	progressReport.ExecutionsPerHour = math.Round(float64(progressReport.TotalExecutionsCount) / hoursPast)
	secondsPast := float64(pastTime) / float64(time.Second)
	progressReport.DatabaseRPS = math.Round(float64(progressReport.TotalDBRequests) / secondsPast)
}

func createShardCleanOutputFiles(shardID int32, cod *CleanOutputDirectories) (*ShardCleanOutputFiles, func(), error) {
	shardCleanReportFile, err := os.Create(fmt.Sprintf("%v/%v", cod.ShardCleanReportDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ShardCleanReportFile: %s", err)
	}
	successfullyCleanedFile, err := os.Create(fmt.Sprintf("%v/%v", cod.SuccessfullyCleanedDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create SuccessfullyCleanedFile: %s", err)
	}
	failedCleanedFile, err := os.Create(fmt.Sprintf("%v/%v", cod.FailedCleanedDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create FailedCleanedFile: %s", err)
	}

	deferFn := func() {
		shardCleanReportFile.Close()
		successfullyCleanedFile.Close()
		failedCleanedFile.Close()
	}
	return &ShardCleanOutputFiles{
		ShardCleanReportFile:    shardCleanReportFile,
		SuccessfullyCleanedFile: successfullyCleanedFile,
		FailedCleanedFile:       failedCleanedFile,
	}, deferFn, nil
}

func createCleanOutputDirectories() (*CleanOutputDirectories, error) {
	now := time.Now().UTC().Unix()
	cod := &CleanOutputDirectories{
		ShardCleanReportDirectoryPath:    fmt.Sprintf("./clean_%v/shard_clean_report", now),
		SuccessfullyCleanedDirectoryPath: fmt.Sprintf("./clean_%v/successfully_cleaned", now),
		FailedCleanedDirectoryPath:       fmt.Sprintf("./clean_%v/failed_cleaned", now),
	}
	if err := os.MkdirAll(cod.ShardCleanReportDirectoryPath, 0766); err != nil {
		return nil, fmt.Errorf("failed to create ShardCleanReportDirectoryPath: %s", err)
	}
	if err := os.MkdirAll(cod.SuccessfullyCleanedDirectoryPath, 0766); err != nil {
		return nil, fmt.Errorf("failed to create SuccessfullyCleanedDirectoryPath: %s", err)
	}
	if err := os.MkdirAll(cod.FailedCleanedDirectoryPath, 0766); err != nil {
		return nil, fmt.Errorf("failed to create FailedCleanedDirectoryPath: %s", err)
	}
	fmt.Println("clean results located under: ", fmt.Sprintf("./clean_%v", now))
	return cod, nil
}

func recordShardCleanReport(file *os.File, sdr *ShardCleanReport) error {
	data, err := json.Marshal(sdr)
	if err != nil {
		return fmt.Errorf("failed to marshal ShardCleanReport: %s", err)
	}
	writeToFile(file, string(data))
	return nil
}

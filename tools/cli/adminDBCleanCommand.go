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

package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/urfave/cli"

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

// AdminDBClean is the command to clean up executions
func AdminDBClean(c *cli.Context) {
	lowerShardBound := int32(c.Int(FlagLowerShardBound))
	upperShardBound := int32(c.Int(FlagUpperShardBound))
	numShards := upperShardBound - lowerShardBound
	startingRPS := c.Int(FlagStartingRPS)
	targetRPS := c.Int(FlagRPS)
	scanWorkerCount := int32(c.Int(FlagConcurrency))
	scanReportRate := int32(c.Int(FlagReportRate))
	if numShards < scanWorkerCount {
		scanWorkerCount = numShards
	}
	inputDirectory := getRequiredOption(c, FlagInputDirectory)

	rateLimiter := getRateLimiter(startingRPS, targetRPS)
	session := connectToCassandra(c)
	defer session.Close()
	cleanOutputDirectories := createCleanOutputDirectories()

	reports := make(chan *ShardCleanReport)
	for i := int32(0); i < scanWorkerCount; i++ {
		go func(workerIdx int32) {
			for shardID := lowerShardBound; shardID < upperShardBound; shardID++ {
				if shardID%scanWorkerCount == workerIdx {
					reports <- cleanShard(
						rateLimiter,
						session,
						cleanOutputDirectories,
						inputDirectory,
						shardID,
					)
				}
			}
		}(i)
	}

	startTime := time.Now().UTC()
	progressReport := &CleanProgressReport{}
	for i := int32(0); i < numShards; i++ {
		report := <-reports
		includeShardCleanInProgressReport(report, progressReport, startTime)
		if i%scanReportRate == 0 || i == numShards-1 {
			reportBytes, err := json.MarshalIndent(*progressReport, "", "\t")
			if err != nil {
				ErrorAndExit("failed to print progress", err)
			}
			fmt.Println(string(reportBytes))
		}
	}
}

func cleanShard(
	limiter quotas.RateLimiter,
	session gocql.Session,
	outputDirectories *CleanOutputDirectories,
	inputDirectory string,
	shardID int32,
) *ShardCleanReport {
	outputFiles, closeFn := createShardCleanOutputFiles(shardID, outputDirectories)
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
		return report
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
		err = execStore.DeleteWorkflowExecution(deleteConcreteReq)
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
			execStore.DeleteCurrentWorkflowExecution(deleteCurrentReq)
		}
		// TODO: we will want to also cleanup history for corrupted workflows, this will be punted on until this is converted to a workflow
	}
	return report
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

func createShardCleanOutputFiles(shardID int32, cod *CleanOutputDirectories) (*ShardCleanOutputFiles, func()) {
	shardCleanReportFile, err := os.Create(fmt.Sprintf("%v/%v", cod.ShardCleanReportDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create ShardCleanReportFile", err)
	}
	successfullyCleanedFile, err := os.Create(fmt.Sprintf("%v/%v", cod.SuccessfullyCleanedDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create SuccessfullyCleanedFile", err)
	}
	failedCleanedFile, err := os.Create(fmt.Sprintf("%v/%v", cod.FailedCleanedDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create FailedCleanedFile", err)
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
	}, deferFn
}

func createCleanOutputDirectories() *CleanOutputDirectories {
	now := time.Now().UTC().Unix()
	cod := &CleanOutputDirectories{
		ShardCleanReportDirectoryPath:    fmt.Sprintf("./clean_%v/shard_clean_report", now),
		SuccessfullyCleanedDirectoryPath: fmt.Sprintf("./clean_%v/successfully_cleaned", now),
		FailedCleanedDirectoryPath:       fmt.Sprintf("./clean_%v/failed_cleaned", now),
	}
	if err := os.MkdirAll(cod.ShardCleanReportDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create ShardCleanReportDirectoryPath", err)
	}
	if err := os.MkdirAll(cod.SuccessfullyCleanedDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create SuccessfullyCleanedDirectoryPath", err)
	}
	if err := os.MkdirAll(cod.FailedCleanedDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create FailedCleanedDirectoryPath", err)
	}
	fmt.Println("clean results located under: ", fmt.Sprintf("./clean_%v", now))
	return cod
}

func recordShardCleanReport(file *os.File, sdr *ShardCleanReport) {
	data, err := json.Marshal(sdr)
	if err != nil {
		ErrorAndExit("failed to marshal ShardCleanReport", err)
	}
	writeToFile(file, string(data))
}

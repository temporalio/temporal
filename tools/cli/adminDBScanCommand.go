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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	enumsspb "github.com/temporalio/temporal/api/enums/v1"
	"github.com/temporalio/temporal/api/persistenceblobs/v1"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/codec"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/persistence"
	cassp "github.com/temporalio/temporal/common/persistence/cassandra"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/quotas"
)

type (
	// CorruptionType indicates the type of corruption that was found
	CorruptionType string
	// VerificationResult is the result of running a verification
	VerificationResult int
)

const (
	// HistoryMissing is the CorruptionType indicating that history is missing
	HistoryMissing CorruptionType = "history_missing"
	// InvalidFirstEvent is the CorruptionType indicating that the first event is invalid
	InvalidFirstEvent = "invalid_first_event"
	// OpenExecutionInvalidCurrentExecution is the CorruptionType that indicates there is an orphan concrete execution
	OpenExecutionInvalidCurrentExecution = "open_execution_invalid_current_execution"
)

const (
	// VerificationResultNoCorruption indicates that no corruption was found
	VerificationResultNoCorruption VerificationResult = iota
	// VerificationResultDetectedCorruption indicates a corruption was found
	VerificationResultDetectedCorruption
	// VerificationResultCheckFailure indicates there was a failure to check corruption
	VerificationResultCheckFailure
)

const (
	historyPageSize = 1
)

type (
	// ScanOutputDirectories are the directory paths for output of scan
	ScanOutputDirectories struct {
		ShardScanReportDirectoryPath       string
		ExecutionCheckFailureDirectoryPath string
		CorruptedExecutionDirectoryPath    string
	}

	// ShardScanOutputFiles are the files produced for a scan of a single shard
	ShardScanOutputFiles struct {
		ShardScanReportFile       *os.File
		ExecutionCheckFailureFile *os.File
		CorruptedExecutionFile    *os.File
	}

	// CorruptedExecution is the type that gets written to CorruptedExecutionFile
	CorruptedExecution struct {
		ShardID                    int
		NamespaceID                string
		WorkflowID                 string
		RunID                      string
		NextEventID                int64
		TreeID                     primitives.UUID
		BranchID                   primitives.UUID
		CloseStatus                enumspb.WorkflowExecutionStatus
		CorruptedExceptionMetadata CorruptedExceptionMetadata
	}

	// CorruptedExceptionMetadata is the metadata for a CorruptedExecution
	CorruptedExceptionMetadata struct {
		CorruptionType CorruptionType
		Note           string
		Details        string
	}

	// ExecutionCheckFailure is the type that gets written to ExecutionCheckFailureFile
	ExecutionCheckFailure struct {
		ShardID     int
		NamespaceID string
		WorkflowID  string
		RunID       string
		Note        string
		Details     string
	}

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
	}

	// ShardScanReportFailure is the part of the ShardScanReport that indicates failure to scan all or part of the shard
	ShardScanReportFailure struct {
		Note    string
		Details string
	}

	// ProgressReport contains metadata about the scan for all shards which have been finished
	// This is periodically printed to stdout
	ProgressReport struct {
		NumberOfShardsFinished           int
		TotalExecutionsCount             int64
		CorruptedExecutionsCount         int64
		ExecutionCheckFailureCount       int64
		NumberOfShardScanFailures        int64
		PercentageCorrupted              float64
		PercentageCheckFailure           float64
		Rates                            Rates
		CorruptionTypeBreakdown          CorruptionTypeBreakdown
		ShardExecutionCountsDistribution ShardExecutionCountsDistribution
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

	// Rates indicates the rates at which the scan is progressing
	Rates struct {
		TimeRunning       string
		DatabaseRPS       float64
		TotalDBRequests   int64
		ShardsPerHour     float64
		ExecutionsPerHour float64
	}

	// ShardExecutionCountsDistribution breaks down stats on the distribution of executions per shard
	ShardExecutionCountsDistribution struct {
		MinExecutions     *int64
		MaxExecutions     *int64
		AverageExecutions int64
	}

	historyBranchByteKey struct {
		TreeID   []byte
		BranchID []byte
	}
)

func byteKeyFromProto(p *persistenceblobs.HistoryBranch) (*historyBranchByteKey, error) {
	branchBytes, err := primitives.ParseUUID(p.BranchId)
	if err != nil {
		return nil, err
	}

	treeBytes, err := primitives.ParseUUID(p.TreeId)
	if err != nil {
		return nil, err
	}

	return &historyBranchByteKey{TreeID: treeBytes, BranchID: branchBytes}, nil
}

func (h *historyBranchByteKey) GetTreeId() primitives.UUID {
	return h.TreeID
}

func (h *historyBranchByteKey) GetBranchId() primitives.UUID {
	return h.BranchID
}

// AdminDBScan is used to scan over all executions in database and detect corruptions
func AdminDBScan(c *cli.Context) {
	lowerShardBound := c.Int(FlagLowerShardBound)
	upperShardBound := c.Int(FlagUpperShardBound)
	numShards := upperShardBound - lowerShardBound
	startingRPS := c.Int(FlagStartingRPS)
	targetRPS := c.Int(FlagRPS)
	scaleUpSeconds := c.Int(FlagRPSScaleUpSeconds)
	scanWorkerCount := c.Int(FlagConcurrency)
	executionsPageSize := c.Int(FlagPageSize)
	scanReportRate := c.Int(FlagReportRate)
	if numShards < scanWorkerCount {
		scanWorkerCount = numShards
	}

	payloadSerializer := persistence.NewPayloadSerializer()
	rateLimiter := getRateLimiter(startingRPS, targetRPS, scaleUpSeconds)
	session := connectToCassandra(c)
	defer session.Close()
	historyStore := cassp.NewHistoryV2PersistenceFromSession(session, loggerimpl.NewNopLogger())
	branchDecoder := codec.NewJSONPBEncoder()
	scanOutputDirectories := createScanOutputDirectories()

	reports := make(chan *ShardScanReport)
	for i := 0; i < scanWorkerCount; i++ {
		go func(workerIdx int) {
			for shardID := lowerShardBound; shardID < upperShardBound; shardID++ {
				if shardID%scanWorkerCount == workerIdx {
					reports <- scanShard(
						session,
						shardID,
						scanOutputDirectories,
						rateLimiter,
						executionsPageSize,
						payloadSerializer,
						historyStore,
						branchDecoder)
				}
			}
		}(i)
	}

	startTime := time.Now()
	progressReport := &ProgressReport{}
	for i := 0; i < numShards; i++ {
		report := <-reports
		includeShardInProgressReport(report, progressReport, startTime)
		if i%scanReportRate == 0 || i == numShards-1 {
			reportBytes, err := json.MarshalIndent(*progressReport, "", "\t")
			if err != nil {
				ErrorAndExit("failed to print progress", err)
			}
			fmt.Println(string(reportBytes))
		}
	}
}

func scanShard(
	session *gocql.Session,
	shardID int,
	scanOutputDirectories *ScanOutputDirectories,
	limiter *quotas.DynamicRateLimiter,
	executionsPageSize int,
	payloadSerializer persistence.PayloadSerializer,
	historyStore persistence.HistoryStore,
	branchDecoder *codec.JSONPBEncoder,
) *ShardScanReport {
	outputFiles, closeFn := createShardScanOutputFiles(shardID, scanOutputDirectories)
	report := &ShardScanReport{
		ShardID: shardID,
	}
	checkFailureWriter := NewBufferedWriter(outputFiles.ExecutionCheckFailureFile)
	corruptedExecutionWriter := NewBufferedWriter(outputFiles.CorruptedExecutionFile)
	defer func() {
		checkFailureWriter.Flush()
		corruptedExecutionWriter.Flush()
		recordShardScanReport(outputFiles.ShardScanReportFile, report)
		deleteEmptyFiles(outputFiles.CorruptedExecutionFile, outputFiles.ExecutionCheckFailureFile, outputFiles.ShardScanReportFile)
		closeFn()
	}()
	execStore, err := cassp.NewWorkflowExecutionPersistence(shardID, session, loggerimpl.NewNopLogger())
	if err != nil {
		report.Failure = &ShardScanReportFailure{
			Note:    "failed to create execution store",
			Details: err.Error(),
		}
		return report
	}

	var token []byte
	isFirstIteration := true
	for isFirstIteration || len(token) != 0 {
		isFirstIteration = false
		req := &persistence.ListConcreteExecutionsRequest{
			PageSize:  executionsPageSize,
			PageToken: token,
		}
		preconditionForDBCall(&report.TotalDBRequests, limiter)
		resp, err := execStore.ListConcreteExecutions(req)
		if err != nil {
			report.Failure = &ShardScanReportFailure{
				Note:    "failed to call ListConcreteExecutions",
				Details: err.Error(),
			}
			return report
		}
		token = resp.NextPageToken
		for _, e := range resp.ExecutionInfos {
			if report.Scanned == nil {
				report.Scanned = &ShardScanReportExecutionsScanned{}
			}
			report.Scanned.TotalExecutionsCount++
			historyVerificationResult, history, historyBranch := verifyHistoryExists(
				e,
				branchDecoder,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				limiter,
				historyStore,
				&report.TotalDBRequests,
				execStore)
			switch historyVerificationResult {
			case VerificationResultNoCorruption:
				// nothing to do just keep checking other conditions
			case VerificationResultDetectedCorruption:
				report.Scanned.CorruptedExecutionsCount++
				report.Scanned.CorruptionTypeBreakdown.TotalHistoryMissing++
				continue
			case VerificationResultCheckFailure:
				report.Scanned.ExecutionCheckFailureCount++
				continue
			}

			if history == nil || historyBranch == nil {
				continue
			}

			firstHistoryEventVerificationResult := verifyFirstHistoryEvent(
				e,
				historyBranch,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				payloadSerializer,
				history)
			switch firstHistoryEventVerificationResult {
			case VerificationResultNoCorruption:
				// nothing to do just keep checking other conditions
			case VerificationResultDetectedCorruption:
				report.Scanned.CorruptionTypeBreakdown.TotalInvalidFirstEvent++
				report.Scanned.CorruptedExecutionsCount++
				continue
			case VerificationResultCheckFailure:
				report.Scanned.ExecutionCheckFailureCount++
				continue
			}

			currentExecutionVerificationResult := verifyCurrentExecution(
				e,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				historyBranch,
				execStore,
				limiter,
				&report.TotalDBRequests)
			switch currentExecutionVerificationResult {
			case VerificationResultNoCorruption:
				// nothing to do just keep checking other conditions
			case VerificationResultDetectedCorruption:
				report.Scanned.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution++
				report.Scanned.CorruptedExecutionsCount++
				continue
			case VerificationResultCheckFailure:
				report.Scanned.ExecutionCheckFailureCount++
				continue
			}
		}
	}
	return report
}

func verifyHistoryExists(
	execution *persistence.InternalWorkflowExecutionInfo,
	branchDecoder *codec.JSONPBEncoder,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int,
	limiter *quotas.DynamicRateLimiter,
	historyStore persistence.HistoryStore,
	totalDBRequests *int64,
	execStore persistence.ExecutionStore,
) (VerificationResult, *persistence.InternalReadHistoryBranchResponse, *persistenceblobs.HistoryBranch) {
	var branch persistenceblobs.HistoryBranch
	err := branchDecoder.Decode(execution.BranchToken, &branch)
	byteBranch, err := byteKeyFromProto(&branch)
	if err != nil {
		return VerificationResultCheckFailure, nil, nil
	}
	if err != nil {
		checkFailureWriter.Add(&ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			Note:        "failed to decode branch token",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure, nil, nil
	}
	readHistoryBranchReq := &persistence.InternalReadHistoryBranchRequest{
		TreeID:    branch.GetTreeId(),
		BranchID:  branch.GetBranchId(),
		MinNodeID: common.FirstEventID,
		MaxNodeID: common.EndEventID,
		ShardID:   shardID,
		PageSize:  historyPageSize,
	}
	preconditionForDBCall(totalDBRequests, limiter)
	history, err := historyStore.ReadHistoryBranch(readHistoryBranchReq)

	ecf, stillExists := concreteExecutionStillExists(execution, shardID, execStore, limiter, totalDBRequests)
	if ecf != nil {
		checkFailureWriter.Add(ecf)
		return VerificationResultCheckFailure, nil, nil
	}
	if !stillExists {
		return VerificationResultNoCorruption, nil, nil
	}

	if err != nil {
		if err == gocql.ErrNotFound {
			corruptedExecutionWriter.Add(&CorruptedExecution{
				ShardID:     shardID,
				NamespaceID: execution.NamespaceID,
				WorkflowID:  execution.WorkflowID,
				RunID:       execution.RunID,
				NextEventID: execution.NextEventID,
				TreeID:      byteBranch.GetTreeId(),
				BranchID:    byteBranch.GetBranchId(),
				CloseStatus: execution.Status,
				CorruptedExceptionMetadata: CorruptedExceptionMetadata{
					CorruptionType: HistoryMissing,
					Note:           "detected history missing based on gocql.ErrNotFound",
					Details:        err.Error(),
				},
			})
			return VerificationResultDetectedCorruption, nil, nil
		}
		checkFailureWriter.Add(&ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			Note:        "failed to read history branch with error other than gocql.ErrNotFond",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure, nil, nil
	} else if history == nil || len(history.History) == 0 {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			NextEventID: execution.NextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: execution.Status,
			CorruptedExceptionMetadata: CorruptedExceptionMetadata{
				CorruptionType: HistoryMissing,
				Note:           "got empty history",
			},
		})
		return VerificationResultDetectedCorruption, nil, nil
	}
	return VerificationResultNoCorruption, history, &branch
}

func verifyFirstHistoryEvent(
	execution *persistence.InternalWorkflowExecutionInfo,
	branch *persistenceblobs.HistoryBranch,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int,
	payloadSerializer persistence.PayloadSerializer,
	history *persistence.InternalReadHistoryBranchResponse,
) VerificationResult {
	byteBranch, err := byteKeyFromProto(branch)
	if err != nil {
		return VerificationResultCheckFailure
	}
	firstBatch, err := payloadSerializer.DeserializeBatchEvents(history.History[0])
	if err != nil || len(firstBatch) == 0 {
		checkFailureWriter.Add(&ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			Note:        "failed to deserialize batch events",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure
	} else if firstBatch[0].GetEventId() != common.FirstEventID {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			NextEventID: execution.NextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: execution.Status,
			CorruptedExceptionMetadata: CorruptedExceptionMetadata{
				CorruptionType: InvalidFirstEvent,
				Note:           "got unexpected first eventID",
				Details:        fmt.Sprintf("expected: %v but got %v", common.FirstEventID, firstBatch[0].GetEventId()),
			},
		})
		return VerificationResultDetectedCorruption
	} else if firstBatch[0].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			NextEventID: execution.NextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: execution.Status,
			CorruptedExceptionMetadata: CorruptedExceptionMetadata{
				CorruptionType: InvalidFirstEvent,
				Note:           "got unexpected first eventType",
				Details:        fmt.Sprintf("expected: %v but got %v", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED.String(), firstBatch[0].GetEventType().String()),
			},
		})
		return VerificationResultDetectedCorruption
	}
	return VerificationResultNoCorruption
}

func verifyCurrentExecution(
	execution *persistence.InternalWorkflowExecutionInfo,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int,
	branch *persistenceblobs.HistoryBranch,
	execStore persistence.ExecutionStore,
	limiter *quotas.DynamicRateLimiter,
	totalDBRequests *int64,
) VerificationResult {
	byteBranch, err := byteKeyFromProto(branch)
	if err != nil {
		return VerificationResultCheckFailure
	}
	if !executionOpen(execution) {
		return VerificationResultNoCorruption
	}
	getCurrentExecutionRequest := &persistence.GetCurrentExecutionRequest{
		NamespaceID: execution.NamespaceID,
		WorkflowID:  execution.WorkflowID,
	}
	preconditionForDBCall(totalDBRequests, limiter)
	currentExecution, err := execStore.GetCurrentExecution(getCurrentExecutionRequest)

	ecf, stillOpen := concreteExecutionStillOpen(execution, shardID, execStore, limiter, totalDBRequests)
	if ecf != nil {
		checkFailureWriter.Add(ecf)
		return VerificationResultCheckFailure
	}
	if !stillOpen {
		return VerificationResultNoCorruption
	}

	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			corruptedExecutionWriter.Add(&CorruptedExecution{
				ShardID:     shardID,
				NamespaceID: execution.NamespaceID,
				WorkflowID:  execution.WorkflowID,
				RunID:       execution.RunID,
				NextEventID: execution.NextEventID,
				TreeID:      byteBranch.GetTreeId(),
				BranchID:    byteBranch.GetBranchId(),
				CloseStatus: execution.Status,
				CorruptedExceptionMetadata: CorruptedExceptionMetadata{
					CorruptionType: OpenExecutionInvalidCurrentExecution,
					Note:           "execution is open without having a current execution",
					Details:        err.Error(),
				},
			})
			return VerificationResultDetectedCorruption
		default:
			checkFailureWriter.Add(&ExecutionCheckFailure{
				ShardID:     shardID,
				NamespaceID: execution.NamespaceID,
				WorkflowID:  execution.WorkflowID,
				RunID:       execution.RunID,
				Note:        "failed to access current execution but could not confirm that it does not exist",
				Details:     err.Error(),
			})
			return VerificationResultCheckFailure
		}
	} else if currentExecution.RunID != execution.RunID {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			NextEventID: execution.NextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: execution.Status,
			CorruptedExceptionMetadata: CorruptedExceptionMetadata{
				CorruptionType: OpenExecutionInvalidCurrentExecution,
				Note:           "found open execution for which there exists current execution pointing at a different concrete execution",
			},
		})
		return VerificationResultDetectedCorruption
	}
	return VerificationResultNoCorruption
}

func concreteExecutionStillExists(
	execution *persistence.InternalWorkflowExecutionInfo,
	shardID int,
	execStore persistence.ExecutionStore,
	limiter *quotas.DynamicRateLimiter,
	totalDBRequests *int64,
) (*ExecutionCheckFailure, bool) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: execution.NamespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowID,
			RunId:      execution.RunID,
		},
	}
	preconditionForDBCall(totalDBRequests, limiter)
	_, err := execStore.GetWorkflowExecution(getConcreteExecution)
	if err == nil {
		return nil, true
	}

	switch err.(type) {
	case *serviceerror.NotFound:
		return nil, false
	default:
		return &ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			Note:        "failed to verify that concrete execution still exists",
			Details:     err.Error(),
		}, false
	}
}

func concreteExecutionStillOpen(
	execution *persistence.InternalWorkflowExecutionInfo,
	shardID int,
	execStore persistence.ExecutionStore,
	limiter *quotas.DynamicRateLimiter,
	totalDBRequests *int64,
) (*ExecutionCheckFailure, bool) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: execution.NamespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowID,
			RunId:      execution.RunID,
		},
	}
	preconditionForDBCall(totalDBRequests, limiter)
	ce, err := execStore.GetWorkflowExecution(getConcreteExecution)
	if err != nil {
		return &ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: execution.NamespaceID,
			WorkflowID:  execution.WorkflowID,
			RunID:       execution.RunID,
			Note:        "failed to access concrete execution to verify it is still open",
			Details:     err.Error(),
		}, false
	}

	return nil, executionOpen(ce.State.ExecutionInfo)
}

func deleteEmptyFiles(files ...*os.File) {
	shouldDelete := func(filepath string) bool {
		fi, err := os.Stat(filepath)
		return err == nil && fi.Size() == 0
	}
	for _, f := range files {
		if shouldDelete(f.Name()) {
			os.Remove(f.Name())
		}
	}
}

func createShardScanOutputFiles(shardID int, sod *ScanOutputDirectories) (*ShardScanOutputFiles, func()) {
	executionCheckFailureFile, err := os.Create(fmt.Sprintf("%v/%v", sod.ExecutionCheckFailureDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create executionCheckFailureFile", err)
	}
	shardScanReportFile, err := os.Create(fmt.Sprintf("%v/%v", sod.ShardScanReportDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create shardScanReportFile", err)
	}
	corruptedExecutionFile, err := os.Create(fmt.Sprintf("%v/%v", sod.CorruptedExecutionDirectoryPath, constructFileNameFromShard(shardID)))
	if err != nil {
		ErrorAndExit("failed to create corruptedExecutionFile", err)
	}

	deferFn := func() {
		executionCheckFailureFile.Close()
		shardScanReportFile.Close()
		corruptedExecutionFile.Close()
	}
	return &ShardScanOutputFiles{
		ShardScanReportFile:       shardScanReportFile,
		ExecutionCheckFailureFile: executionCheckFailureFile,
		CorruptedExecutionFile:    corruptedExecutionFile,
	}, deferFn
}

func constructFileNameFromShard(shardID int) string {
	return fmt.Sprintf("shard_%v.json", shardID)
}

func createScanOutputDirectories() *ScanOutputDirectories {
	now := time.Now().Unix()
	sod := &ScanOutputDirectories{
		ShardScanReportDirectoryPath:       fmt.Sprintf("./scan_%v/shard_scan_report", now),
		ExecutionCheckFailureDirectoryPath: fmt.Sprintf("./scan_%v/execution_check_failure", now),
		CorruptedExecutionDirectoryPath:    fmt.Sprintf("./scan_%v/corrupted_execution", now),
	}
	if err := os.MkdirAll(sod.ShardScanReportDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create ShardScanFailureDirectoryPath", err)
	}
	if err := os.MkdirAll(sod.ExecutionCheckFailureDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create ExecutionCheckFailureDirectoryPath", err)
	}
	if err := os.MkdirAll(sod.CorruptedExecutionDirectoryPath, 0766); err != nil {
		ErrorAndExit("failed to create CorruptedExecutionDirectoryPath", err)
	}
	fmt.Println("scan results located under: ", fmt.Sprintf("./scan_%v", now))
	return sod
}

func recordShardScanReport(file *os.File, ssr *ShardScanReport) {
	data, err := json.Marshal(ssr)
	if err != nil {
		ErrorAndExit("failed to marshal ShardScanReport", err)
	}
	writeToFile(file, string(data))
}

func writeToFile(file *os.File, message string) {
	if _, err := file.WriteString(fmt.Sprintf("%v\r\n", message)); err != nil {
		ErrorAndExit("failed to write to file", err)
	}
}

func includeShardInProgressReport(report *ShardScanReport, progressReport *ProgressReport, startTime time.Time) {
	progressReport.NumberOfShardsFinished++
	progressReport.Rates.TotalDBRequests += report.TotalDBRequests
	progressReport.Rates.TimeRunning = time.Now().Sub(startTime).String()
	if report.Failure != nil {
		progressReport.NumberOfShardScanFailures++
	}
	if report.Scanned != nil {
		progressReport.CorruptedExecutionsCount += report.Scanned.CorruptedExecutionsCount
		progressReport.TotalExecutionsCount += report.Scanned.TotalExecutionsCount
		progressReport.ExecutionCheckFailureCount += report.Scanned.ExecutionCheckFailureCount
		progressReport.CorruptionTypeBreakdown.TotalHistoryMissing += report.Scanned.CorruptionTypeBreakdown.TotalHistoryMissing
		progressReport.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution += report.Scanned.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution
		progressReport.CorruptionTypeBreakdown.TotalInvalidFirstEvent += report.Scanned.CorruptionTypeBreakdown.TotalInvalidFirstEvent
		if progressReport.ShardExecutionCountsDistribution.MinExecutions == nil ||
			*progressReport.ShardExecutionCountsDistribution.MinExecutions > report.Scanned.TotalExecutionsCount {
			progressReport.ShardExecutionCountsDistribution.MinExecutions = &report.Scanned.TotalExecutionsCount
		}
		if progressReport.ShardExecutionCountsDistribution.MaxExecutions == nil ||
			*progressReport.ShardExecutionCountsDistribution.MaxExecutions < report.Scanned.TotalExecutionsCount {
			progressReport.ShardExecutionCountsDistribution.MaxExecutions = &report.Scanned.TotalExecutionsCount
		}
		progressReport.ShardExecutionCountsDistribution.AverageExecutions = progressReport.TotalExecutionsCount / int64(progressReport.NumberOfShardsFinished)
	}

	if progressReport.TotalExecutionsCount > 0 {
		progressReport.PercentageCorrupted = math.Round((float64(progressReport.CorruptedExecutionsCount) * 100.0) / float64(progressReport.TotalExecutionsCount))
		progressReport.PercentageCheckFailure = math.Round((float64(progressReport.ExecutionCheckFailureCount) * 100.0) / float64(progressReport.TotalExecutionsCount))
		progressReport.CorruptionTypeBreakdown.PercentageHistoryMissing = math.Round((float64(progressReport.CorruptionTypeBreakdown.TotalHistoryMissing) * 100.0) / float64(progressReport.TotalExecutionsCount))
		progressReport.CorruptionTypeBreakdown.PercentageInvalidStartEvent = math.Round((float64(progressReport.CorruptionTypeBreakdown.TotalInvalidFirstEvent) * 100.0) / float64(progressReport.TotalExecutionsCount))
		progressReport.CorruptionTypeBreakdown.PercentageOpenExecutionInvalidCurrentExecution = math.Round((float64(progressReport.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution) * 100.0) / float64(progressReport.TotalExecutionsCount))
	}

	pastTime := time.Now().Sub(startTime)
	hoursPast := float64(pastTime) / float64(time.Hour)
	progressReport.Rates.ShardsPerHour = math.Round(float64(progressReport.NumberOfShardsFinished) / hoursPast)
	progressReport.Rates.ExecutionsPerHour = math.Round(float64(progressReport.TotalExecutionsCount) / hoursPast)

	secondsPast := float64(pastTime) / float64(time.Second)
	progressReport.Rates.DatabaseRPS = math.Round(float64(progressReport.Rates.TotalDBRequests) / secondsPast)
}

func getRateLimiter(startRPS int, targetRPS int, scaleUpSeconds int) *quotas.DynamicRateLimiter {
	if startRPS >= targetRPS {
		ErrorAndExit("startRPS is greater than target RPS", nil)
	}
	if scaleUpSeconds == 0 {
		return quotas.NewDynamicRateLimiter(func() float64 { return float64(targetRPS) })
	}
	rpsIncreasePerSecond := (targetRPS - startRPS) / scaleUpSeconds
	startTime := time.Now()
	rpsFn := func() float64 {
		secondsPast := int(time.Now().Sub(startTime).Seconds())
		if secondsPast >= scaleUpSeconds {
			return float64(targetRPS)
		}
		return float64((rpsIncreasePerSecond * secondsPast) + startRPS)
	}
	return quotas.NewDynamicRateLimiter(rpsFn)
}

func preconditionForDBCall(totalDBRequests *int64, limiter *quotas.DynamicRateLimiter) {
	*totalDBRequests = *totalDBRequests + 1
	limiter.Wait(context.Background())
}

func executionOpen(execution *persistence.InternalWorkflowExecutionInfo) bool {
	return execution.State == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED || execution.State == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
}

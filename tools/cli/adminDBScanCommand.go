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

	"github.com/urfave/cli"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	cassp "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
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
	CorruptActivityIdPresent             = "corrupt_activity_id_present"
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
		ShardID                    int32
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
		ShardID     int32
		NamespaceID string
		WorkflowID  string
		RunID       string
		Note        string
		Details     string
	}

	// ShardScanReport is the type that gets written to ShardScanReportFile
	ShardScanReport struct {
		ShardID         int32
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
		TotalActivityIdsCorrupted                      int64
		PercentageHistoryMissing                       float64
		PercentageInvalidStartEvent                    float64
		PercentageOpenExecutionInvalidCurrentExecution float64
		PercentageActivityIdsCorrupted                 float64
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

func byteKeyFromProto(p *persistencespb.HistoryBranch) (*historyBranchByteKey, error) {
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
	lowerShardBound := int32(c.Int(FlagLowerShardBound))
	upperShardBound := int32(c.Int(FlagUpperShardBound))
	numShards := upperShardBound - lowerShardBound
	startingRPS := c.Int(FlagStartingRPS)
	targetRPS := c.Int(FlagRPS)
	scanWorkerCount := int32(c.Int(FlagConcurrency))
	executionsPageSize := c.Int(FlagPageSize)
	scanReportRate := int32(c.Int(FlagReportRate))
	if numShards < scanWorkerCount {
		scanWorkerCount = numShards
	}

	payloadSerializer := serialization.NewSerializer()
	rateLimiter := getRateLimiter(startingRPS, targetRPS)
	session := connectToCassandra(c)
	defer session.Close()
	scanOutputDirectories := createScanOutputDirectories()

	reports := make(chan *ShardScanReport)
	for i := int32(0); i < scanWorkerCount; i++ {
		go func(workerIdx int32) {
			for shardID := lowerShardBound; shardID < upperShardBound; shardID++ {
				if shardID%scanWorkerCount == workerIdx {
					reports <- scanShard(
						session,
						shardID,
						scanOutputDirectories,
						rateLimiter,
						executionsPageSize,
						payloadSerializer)
				}
			}
		}(i)
	}

	startTime := time.Now().UTC()
	progressReport := &ProgressReport{}
	for i := int32(0); i < numShards; i++ {
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
	session gocql.Session,
	shardID int32,
	scanOutputDirectories *ScanOutputDirectories,
	limiter quotas.RateLimiter,
	executionsPageSize int,
	payloadSerializer serialization.Serializer,
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
	workflowStore := cassp.NewExecutionStore(session, log.NewNoopLogger())
	execMan := persistence.NewExecutionManager(workflowStore, log.NewNoopLogger(), dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit))

	var token []byte
	isFirstIteration := true
	for isFirstIteration || len(token) != 0 {
		isFirstIteration = false
		req := &persistence.ListConcreteExecutionsRequest{
			ShardID:   shardID,
			PageSize:  executionsPageSize,
			PageToken: token,
		}
		preconditionForDBCall(&report.TotalDBRequests, limiter)
		resp, err := execMan.ListConcreteExecutions(req)
		if err != nil {
			report.Failure = &ShardScanReportFailure{
				Note:    "failed to call ListConcreteExecutions",
				Details: err.Error(),
			}
			return report
		}
		token = resp.PageToken
		for _, s := range resp.States {
			if report.Scanned == nil {
				report.Scanned = &ShardScanReportExecutionsScanned{}
			}
			report.Scanned.TotalExecutionsCount++
			historyVerificationResult, history, historyBranch := fetchAndVerifyHistoryExists(
				s.ExecutionInfo,
				s.ExecutionState,
				s.NextEventId,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				limiter,
				workflowStore,
				&report.TotalDBRequests,
			)
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

			byteBranch, err := byteKeyFromProto(historyBranch)
			if err != nil {
				report.Scanned.ExecutionCheckFailureCount++
				continue
			}

			firstHistoryEventVerificationResult := verifyFirstHistoryEvent(
				s.ExecutionInfo,
				s.ExecutionState,
				s.NextEventId,
				byteBranch,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				payloadSerializer,
				history,
			)
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
				s.ExecutionInfo,
				s.ExecutionState,
				s.NextEventId,
				corruptedExecutionWriter,
				checkFailureWriter,
				shardID,
				byteBranch,
				execMan,
				limiter,
				&report.TotalDBRequests,
			)
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

			activityIdsVerificationResult := verifyActivityIds(
				shardID,
				s.NextEventId,
				s.ActivityInfos,
				s.ExecutionInfo,
				s.ExecutionState,
				corruptedExecutionWriter,
				historyBranch,
			)
			switch activityIdsVerificationResult {
			case VerificationResultNoCorruption:
			case VerificationResultDetectedCorruption:
				report.Scanned.CorruptionTypeBreakdown.TotalActivityIdsCorrupted++
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

func fetchAndVerifyHistoryExists(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int32,
	limiter quotas.RateLimiter,
	executionStore persistence.ExecutionStore,
	totalDBRequests *int64,
) (VerificationResult, *persistence.InternalReadHistoryBranchResponse, *persistencespb.HistoryBranch) {
	var branch *persistencespb.HistoryBranch
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err == nil {
		branch, err = serialization.HistoryBranchFromBlob(currentVersionHistory.BranchToken,
			enumspb.ENCODING_TYPE_PROTO3.String())
	}

	if err != nil {
		checkFailureWriter.Add(&ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			Note:        "failed to decode branch token",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure, nil, nil
	}

	byteBranch, err := byteKeyFromProto(branch)
	readHistoryBranchReq := &persistence.InternalReadHistoryBranchRequest{
		TreeID:    branch.GetTreeId(),
		BranchID:  branch.GetBranchId(),
		MinNodeID: common.FirstEventID,
		MaxNodeID: common.EndEventID,
		ShardID:   shardID,
		PageSize:  historyPageSize,
	}
	preconditionForDBCall(totalDBRequests, limiter)
	history, err := executionStore.ReadHistoryBranch(readHistoryBranchReq)

	ecf, stillExists := concreteExecutionStillExists(executionInfo, executionState, shardID, executionStore, limiter, totalDBRequests)
	if ecf != nil {
		checkFailureWriter.Add(ecf)
		return VerificationResultCheckFailure, nil, nil
	}
	if !stillExists {
		return VerificationResultNoCorruption, nil, nil
	}

	if err != nil {
		if gocql.IsNotFoundError(err) {
			corruptedExecutionWriter.Add(&CorruptedExecution{
				ShardID:     shardID,
				NamespaceID: executionInfo.NamespaceId,
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.GetRunId(),
				NextEventID: nextEventID,
				TreeID:      byteBranch.GetTreeId(),
				BranchID:    byteBranch.GetBranchId(),
				CloseStatus: executionState.Status,
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
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			Note:        "failed to read history branch with error other than gocql.ErrNotFond",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure, nil, nil
	} else if history == nil || len(history.Nodes) == 0 {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			NextEventID: nextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: executionState.Status,
			CorruptedExceptionMetadata: CorruptedExceptionMetadata{
				CorruptionType: HistoryMissing,
				Note:           "got empty history",
			},
		})
		return VerificationResultDetectedCorruption, nil, nil
	}
	return VerificationResultNoCorruption, history, branch
}

func verifyFirstHistoryEvent(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	byteBranch *historyBranchByteKey,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int32,
	payloadSerializer serialization.Serializer,
	history *persistence.InternalReadHistoryBranchResponse,
) VerificationResult {
	firstBatch, err := payloadSerializer.DeserializeEvents(history.Nodes[0].Events)
	if err != nil || len(firstBatch) == 0 {
		checkFailureWriter.Add(&ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			Note:        "failed to deserialize batch events",
			Details:     err.Error(),
		})
		return VerificationResultCheckFailure
	} else if firstBatch[0].GetEventId() != common.FirstEventID {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			NextEventID: nextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: executionState.Status,
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
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			NextEventID: nextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: executionState.Status,
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

// Checks for validity of activity ids.
// This refers to an accident when DB or our code wrote incorrect values that looked like some huge int64s.
func verifyActivityIds(
	shardID int32,
	nextEventID int64,
	activityInfos map[int64]*persistencespb.ActivityInfo,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	corruptedExecutionWriter BufferedWriter,
	branch *persistencespb.HistoryBranch,
) VerificationResult {
	if len(activityInfos) == 0 {
		return VerificationResultNoCorruption
	}

	for activityId := range activityInfos {
		if activityId >= nextEventID || activityId < 0 {
			byteBranch, err := byteKeyFromProto(branch)
			if err != nil {
				return VerificationResultCheckFailure
			}
			corruptedExecutionWriter.Add(
				&CorruptedExecution{
					ShardID:     shardID,
					NamespaceID: executionInfo.NamespaceId,
					WorkflowID:  executionInfo.WorkflowId,
					RunID:       executionState.GetRunId(),
					NextEventID: nextEventID,
					TreeID:      byteBranch.GetTreeId(),
					BranchID:    byteBranch.GetBranchId(),
					CloseStatus: executionState.Status,
					CorruptedExceptionMetadata: CorruptedExceptionMetadata{
						CorruptionType: CorruptActivityIdPresent,
						Note:           "ActivityID greater than NextEventID present",
						Details:        fmt.Sprint(activityId),
					},
				},
			)
			return VerificationResultDetectedCorruption
		}
	}
	return VerificationResultNoCorruption
}

func verifyCurrentExecution(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	nextEventID int64,
	corruptedExecutionWriter BufferedWriter,
	checkFailureWriter BufferedWriter,
	shardID int32,
	byteBranch *historyBranchByteKey,
	execMan persistence.ExecutionManager,
	limiter quotas.RateLimiter,
	totalDBRequests *int64,
) VerificationResult {
	if !executionOpen(executionState) {
		return VerificationResultNoCorruption
	}
	getCurrentExecutionRequest := &persistence.GetCurrentExecutionRequest{
		ShardID:     shardID,
		NamespaceID: executionInfo.NamespaceId,
		WorkflowID:  executionInfo.WorkflowId,
	}
	preconditionForDBCall(totalDBRequests, limiter)
	currentExecution, err := execMan.GetCurrentExecution(getCurrentExecutionRequest)

	ecf, stillOpen := concreteExecutionStillOpen(executionInfo, executionState, shardID, execMan, limiter, totalDBRequests)
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
				NamespaceID: executionInfo.NamespaceId,
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.GetRunId(),
				NextEventID: nextEventID,
				TreeID:      byteBranch.GetTreeId(),
				BranchID:    byteBranch.GetBranchId(),
				CloseStatus: executionState.Status,
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
				NamespaceID: executionInfo.NamespaceId,
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.GetRunId(),
				Note:        "failed to access current execution but could not confirm that it does not exist",
				Details:     err.Error(),
			})
			return VerificationResultCheckFailure
		}
	} else if currentExecution.RunID != executionState.GetRunId() {
		corruptedExecutionWriter.Add(&CorruptedExecution{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			NextEventID: nextEventID,
			TreeID:      byteBranch.GetTreeId(),
			BranchID:    byteBranch.GetBranchId(),
			CloseStatus: executionState.Status,
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
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	shardID int32,
	executionStore persistence.ExecutionStore,
	limiter quotas.RateLimiter,
	totalDBRequests *int64,
) (*ExecutionCheckFailure, bool) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: executionInfo.NamespaceId,
		WorkflowID:  executionInfo.WorkflowId,
		RunID:       executionState.GetRunId(),
	}
	preconditionForDBCall(totalDBRequests, limiter)
	_, err := executionStore.GetWorkflowExecution(getConcreteExecution)
	if err == nil {
		return nil, true
	}

	switch err.(type) {
	case *serviceerror.NotFound:
		return nil, false
	default:
		return &ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			Note:        "failed to verify that concrete execution still exists",
			Details:     err.Error(),
		}, false
	}
}

func concreteExecutionStillOpen(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	shardID int32,
	execMan persistence.ExecutionManager,
	limiter quotas.RateLimiter,
	totalDBRequests *int64,
) (*ExecutionCheckFailure, bool) {
	getConcreteExecution := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: executionInfo.NamespaceId,
		WorkflowID:  executionInfo.WorkflowId,
		RunID:       executionState.GetRunId(),
	}
	preconditionForDBCall(totalDBRequests, limiter)
	ce, err := execMan.GetWorkflowExecution(getConcreteExecution)
	if err != nil {
		return &ExecutionCheckFailure{
			ShardID:     shardID,
			NamespaceID: executionInfo.NamespaceId,
			WorkflowID:  executionInfo.WorkflowId,
			RunID:       executionState.GetRunId(),
			Note:        "failed to access concrete execution to verify it is still open",
			Details:     err.Error(),
		}, false
	}

	return nil, executionOpen(ce.State.ExecutionState)
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

func createShardScanOutputFiles(shardID int32, sod *ScanOutputDirectories) (*ShardScanOutputFiles, func()) {
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

func constructFileNameFromShard(shardID int32) string {
	return fmt.Sprintf("shard_%v.json", shardID)
}

func createScanOutputDirectories() *ScanOutputDirectories {
	now := time.Now().UTC().Unix()
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
	progressReport.Rates.TimeRunning = time.Now().UTC().Sub(startTime).String()
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

	pastTime := time.Now().UTC().Sub(startTime)
	hoursPast := float64(pastTime) / float64(time.Hour)
	progressReport.Rates.ShardsPerHour = math.Round(float64(progressReport.NumberOfShardsFinished) / hoursPast)
	progressReport.Rates.ExecutionsPerHour = math.Round(float64(progressReport.TotalExecutionsCount) / hoursPast)

	secondsPast := float64(pastTime) / float64(time.Second)
	progressReport.Rates.DatabaseRPS = math.Round(float64(progressReport.Rates.TotalDBRequests) / secondsPast)
}

func getRateLimiter(startRPS int, targetRPS int) quotas.RateLimiter {
	if startRPS >= targetRPS {
		ErrorAndExit("startRPS is greater than target RPS", nil)
	}
	return quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return float64(targetRPS) },
	)
}

func preconditionForDBCall(totalDBRequests *int64, limiter quotas.RateLimiter) {
	*totalDBRequests = *totalDBRequests + 1
	_ = limiter.Wait(context.Background())
}

func executionOpen(executionState *persistencespb.WorkflowExecutionState) bool {
	return executionState.State == enumsspb.WORKFLOW_EXECUTION_STATE_CREATED ||
		executionState.State == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
}

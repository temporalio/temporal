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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
	cassp "github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/quotas"
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

	// ExecutionToRecord is an execution which needs to be recorded
	ExecutionToRecord struct {
		ShardID           int
		DomainID          string
		WorkflowID        string
		RunID             string
		TreeID            string
		BranchID          string
		CloseStatus       int
		State             int
		CheckType         CheckType
		CheckResultStatus CheckResultStatus
		ErrorInfo         *ErrorInfo
	}
)

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
	skipHistoryChecks := c.Bool(FlagSkipHistoryChecks)

	payloadSerializer := persistence.NewPayloadSerializer()
	rateLimiter := getRateLimiter(startingRPS, targetRPS, scaleUpSeconds)
	session := connectToCassandra(c)
	defer session.Close()
	historyStore := cassp.NewHistoryV2PersistenceFromSession(session, loggerimpl.NewNopLogger())
	branchDecoder := codec.NewThriftRWEncoder()
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
						branchDecoder,
						skipHistoryChecks,
						historyStore)
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
	branchDecoder *codec.ThriftRWEncoder,
	skipHistoryChecks bool,
	historyStore persistence.HistoryStore,
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

	checks := getChecks(skipHistoryChecks, limiter, execStore, payloadSerializer, historyStore)

	var token []byte
	isFirstIteration := true
	for isFirstIteration || len(token) != 0 {
		isFirstIteration = false
		req := &persistence.ListConcreteExecutionsRequest{
			PageSize:  executionsPageSize,
			PageToken: token,
		}
		resp, err := retryListConcreteExecutions(limiter, &report.TotalDBRequests, execStore, req)
		if err != nil {
			report.Failure = &ShardScanReportFailure{
				Note:    "failed to call ListConcreteExecutions",
				Details: err.Error(),
			}
			return report
		}
		token = resp.NextPageToken
		for _, e := range resp.Executions {
			if e == nil || e.ExecutionInfo == nil {
				continue
			}
			if report.Scanned == nil {
				report.Scanned = &ShardScanReportExecutionsScanned{}
			}
			report.Scanned.TotalExecutionsCount++

			cr, err := getCheckRequest(shardID, e, payloadSerializer, branchDecoder)
			if err != nil {
				report.Scanned.ExecutionCheckFailureCount++
				checkFailureWriter.Add(&ExecutionToRecord{
					ShardID:           shardID,
					DomainID:          e.ExecutionInfo.DomainID,
					WorkflowID:        e.ExecutionInfo.WorkflowID,
					RunID:             e.ExecutionInfo.RunID,
					CheckResultStatus: CheckResultFailed,
					ErrorInfo: &ErrorInfo{
						Note:    "failed to get check request",
						Details: err.Error(),
					},
				})
				continue
			}
		CheckerLoop:
			for _, c := range checks {
				if !c.ValidRequest(cr) {
					continue
				}
				result := c.Check(cr)
				cr.PrerequisiteCheckPayload = result.Payload

				etr := &ExecutionToRecord{
					ShardID:           shardID,
					DomainID:          e.ExecutionInfo.DomainID,
					WorkflowID:        e.ExecutionInfo.WorkflowID,
					RunID:             e.ExecutionInfo.RunID,
					TreeID:            cr.TreeID,
					BranchID:          cr.BranchID,
					CloseStatus:       e.ExecutionInfo.CloseStatus,
					State:             cr.State,
					CheckType:         result.CheckType,
					CheckResultStatus: result.CheckResultStatus,
					ErrorInfo:         result.ErrorInfo,
				}

				switch result.CheckResultStatus {
				case CheckResultHealthy:
					// nothing to do just keep checking other conditions
				case CheckResultFailed:
					report.Scanned.ExecutionCheckFailureCount++
					checkFailureWriter.Add(etr)
					break CheckerLoop
				case CheckResultCorrupted:
					report.Scanned.CorruptedExecutionsCount++
					switch result.CheckType {
					case CheckTypeOrphanExecution:
						report.Scanned.CorruptionTypeBreakdown.TotalOpenExecutionInvalidCurrentExecution++
					case CheckTypeValidFirstEvent:
						report.Scanned.CorruptionTypeBreakdown.TotalInvalidFirstEvent++
					case CheckTypeHistoryExists:
						report.Scanned.CorruptionTypeBreakdown.TotalHistoryMissing++
					}
					if executionOpen(cr) {
						report.Scanned.OpenCorruptions.TotalOpen++
					}
					corruptedExecutionWriter.Add(etr)
					break CheckerLoop
				}
			}
		}
	}
	return report
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
	if err := os.MkdirAll(sod.ShardScanReportDirectoryPath, 0777); err != nil {
		ErrorAndExit("failed to create ShardScanFailureDirectoryPath", err)
	}
	if err := os.MkdirAll(sod.ExecutionCheckFailureDirectoryPath, 0777); err != nil {
		ErrorAndExit("failed to create ExecutionCheckFailureDirectoryPath", err)
	}
	if err := os.MkdirAll(sod.CorruptedExecutionDirectoryPath, 0777); err != nil {
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

func getCheckRequest(
	shardID int,
	e *persistence.InternalListConcreteExecutionsEntity,
	payloadSerializer persistence.PayloadSerializer,
	branchDecoder *codec.ThriftRWEncoder,
) (*CheckRequest, error) {
	hb, err := getHistoryBranch(e, payloadSerializer, branchDecoder)
	if err != nil {
		return nil, err
	}
	return &CheckRequest{
		ShardID:    shardID,
		DomainID:   e.ExecutionInfo.DomainID,
		WorkflowID: e.ExecutionInfo.WorkflowID,
		RunID:      e.ExecutionInfo.RunID,
		TreeID:     hb.GetTreeID(),
		BranchID:   hb.GetBranchID(),
		State:      e.ExecutionInfo.State,
	}, nil
}

func getHistoryBranch(
	e *persistence.InternalListConcreteExecutionsEntity,
	payloadSerializer persistence.PayloadSerializer,
	branchDecoder *codec.ThriftRWEncoder,
) (*shared.HistoryBranch, error) {
	branchTokenBytes := e.ExecutionInfo.BranchToken
	if len(branchTokenBytes) == 0 {
		if e.VersionHistories == nil {
			return nil, errors.New("failed to get branch token")
		}
		vh, err := payloadSerializer.DeserializeVersionHistories(e.VersionHistories)
		if err != nil {
			return nil, err
		}
		branchTokenBytes = vh.GetHistories()[vh.GetCurrentVersionHistoryIndex()].GetBranchToken()
	}
	var branch shared.HistoryBranch
	if err := branchDecoder.Decode(branchTokenBytes, &branch); err != nil {
		return nil, err
	}
	return &branch, nil
}

func getChecks(
	skipHistoryChecks bool,
	limiter *quotas.DynamicRateLimiter,
	execStore persistence.ExecutionStore,
	payloadSerializer persistence.PayloadSerializer,
	historyStore persistence.HistoryStore,
) []AdminDBCheck {
	// the order in which checks are added to the list is important
	// some checks depend on the output of other checks
	if skipHistoryChecks {
		return []AdminDBCheck{NewOrphanExecutionCheck(limiter, execStore, payloadSerializer)}
	}
	return []AdminDBCheck{
		NewHistoryExistsCheck(limiter, historyStore, execStore),
		NewFirstHistoryEventCheck(payloadSerializer),
		NewOrphanExecutionCheck(limiter, execStore, payloadSerializer),
	}
}

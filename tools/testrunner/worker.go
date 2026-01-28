package testrunner

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// workerPool holds shared state for parallel test execution.
type workerPool struct {
	ctx            context.Context
	cfg            runConfig
	baseArgs       []string
	queue          []testFile
	queueMu        sync.Mutex
	junitReports   []*junitReport
	alerts         []alert
	resultMu       sync.Mutex
	totalFiles     int
	totalBatches   atomic.Int32
	indexWidth     int // width for zero-padding batch index numbers
	nextBatchIndex atomic.Int32
	completed      atomic.Int32
	failed         atomic.Int32
	attemptResults []batchResult // track all batch results for duration reporting
}

// fmtBatchIndex formats a batch index with zero-padding based on total batches.
func (p *workerPool) fmtBatchIndex(index int) string {
	return fmt.Sprintf("[%0*d/%d]", p.indexWidth, index, p.totalBatches.Load())
}

func newWorkerPool(ctx context.Context, cfg runConfig, testFiles []testFile, baseArgs []string) *workerPool {
	// Initialize test files with attempt 1
	for i := range testFiles {
		testFiles[i].attempt = 1
	}

	// Calculate total batches
	totalFiles := len(testFiles)
	totalBatches := (totalFiles + cfg.filesPerWorker - 1) / cfg.filesPerWorker

	// Calculate width needed for index padding (number of digits in total batches)
	indexWidth := 1
	for n := totalBatches; n >= 10; n /= 10 {
		indexWidth++
	}

	p := &workerPool{
		ctx:        ctx,
		cfg:        cfg,
		baseArgs:   baseArgs,
		queue:      append([]testFile(nil), testFiles...),
		totalFiles: totalFiles,
		indexWidth: indexWidth,
	}
	p.totalBatches.Store(int32(totalBatches))
	return p
}

// runResult holds the aggregated results from a worker pool run.
type runResult struct {
	junitReports []*junitReport
	alerts       []alert
	failed       int
	total        int
}

// run executes tests for all files in parallel using worker goroutines.
// It uses a queue-based retry mechanism where failed tests are re-queued with only the failed test names.
func (p *workerPool) run() runResult {
	log.Printf("starting test run: workers=%d, filesPerWorker=%d, batches=%d, files=%d",
		p.cfg.parallelism, p.cfg.filesPerWorker, p.totalBatches.Load(), p.totalFiles)

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.parallelism; i++ {
		workerID := i
		wg.Go(func() { p.runWorker(workerID) })
	}
	wg.Wait()

	return runResult{
		junitReports: p.junitReports,
		alerts:       p.alerts,
		failed:       int(p.failed.Load()),
		total:        int(p.totalBatches.Load()),
	}
}

func (p *workerPool) getBatch() ([]testFile, int) {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	if len(p.queue) == 0 {
		return nil, 0
	}
	batchSize := p.cfg.filesPerWorker
	if batchSize > len(p.queue) {
		batchSize = len(p.queue)
	}
	batch := make([]testFile, batchSize)
	copy(batch, p.queue[:batchSize])
	p.queue = p.queue[batchSize:]
	batchIndex := int(p.nextBatchIndex.Add(1))
	return batch, batchIndex
}

func (p *workerPool) queueRetries(batch []testFile, failedTests []string) {
	if len(failedTests) == 0 {
		return
	}

	// Build a set of top-level test names from failed tests.
	// Failed tests may include subtests like "TestFoo/subtest", but we need to match
	// against top-level function names like "TestFoo".
	failedTopLevel := make(map[string]bool)
	for _, t := range failedTests {
		// Extract top-level test name (before first /)
		if idx := strings.Index(t, "/"); idx > 0 {
			failedTopLevel[t[:idx]] = true
		} else {
			failedTopLevel[t] = true
		}
	}

	// For each file in the batch, check if any of its tests failed
	var retryCount int
	p.queueMu.Lock()
	for _, tf := range batch {
		var fileFailedTests []string
		for _, testName := range tf.testNames {
			if failedTopLevel[testName] {
				fileFailedTests = append(fileFailedTests, testName)
			}
		}
		if len(fileFailedTests) > 0 {
			p.queue = append(p.queue, testFile{
				path:      tf.path,
				pkg:       tf.pkg,
				testNames: fileFailedTests,
				attempt:   tf.attempt + 1,
			})
			retryCount++
		}
	}
	p.queueMu.Unlock()

	// Update total batches count for the retry batches
	if retryCount > 0 {
		newBatches := (retryCount + p.cfg.filesPerWorker - 1) / p.cfg.filesPerWorker
		p.totalBatches.Add(int32(newBatches))
	}
}

func (p *workerPool) processBatchResult(batch []testFile, batchIndex int, result batchResult) {
	// Read junit report to get test duration and results
	var testDuration time.Duration
	var totalTests, failedTests int
	p.resultMu.Lock()
	if result.junitReportPath != "" {
		jr := &junitReport{path: result.junitReportPath, attempt: result.attempt}
		if err := jr.read(); err != nil {
			log.Printf("warning: failed to read junit report %s: %v", result.junitReportPath, err)
		} else {
			testDuration = jr.duration()
			totalTests = jr.Tests
			failedTests = jr.Failures
			p.junitReports = append(p.junitReports, jr)
		}
	}
	p.alerts = append(p.alerts, result.alerts...)
	p.resultMu.Unlock()

	if result.exitCode != 0 {
		if result.attempt < p.cfg.maxAttempts && len(result.failedTests) > 0 {
			p.queueRetries(batch, result.failedTests)
		}
	}

	// Build header for collapsible log group and log results
	desc := batchDesc(batch)
	timestamp := time.Now().Format("15:04:05")
	passed := result.exitCode == 0

	p.completed.Add(int32(len(batch)))
	if !passed {
		p.failed.Add(int32(len(batch)))
	}
	status := "✅"
	if !passed {
		status = "❌️"
	}
	passedTests := totalTests - failedTests
	header := fmt.Sprintf("[runner] %s %s %s %s (attempt=%d, passed=%d/%d, runtime=%v)",
		timestamp, p.fmtBatchIndex(batchIndex), status, desc, result.attempt, passedTests, totalTests, testDuration.Round(time.Second))

	// Flush captured output with header
	if result.capture != nil {
		result.capture.SetHeader(header)
		_ = result.capture.Flush()
	}
}

func (p *workerPool) runWorker(workerID int) {
	for {
		// Stop if context is cancelled
		if p.ctx.Err() != nil {
			return
		}
		batch, batchIndex := p.getBatch()
		if batch == nil {
			return
		}
		log.Printf("%s 🚀 starting %s (attempt=%d)", p.fmtBatchIndex(batchIndex), batchDesc(batch), batch[0].attempt)
		result := execTestBatch(p.ctx, p.cfg, batch, p.baseArgs)
		p.processBatchResult(batch, batchIndex, result)
	}
}

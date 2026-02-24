package flakereport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"
)

const (
	minFlakyFailures    = 3
	defaultMaxLinks     = 3
	defaultLookbackDays = 7
	defaultWorkflowID   = 80591745
	defaultRepository   = "temporalio/temporal"
	defaultBranch       = "main"
	defaultConcurrency  = 20
	slackMaxListItems   = 10
)

// NewCliApp instantiates a new instance of the CLI application
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "flakesreport"
	app.Usage = "Generate flaky test reports"
	app.Version = headers.ServerVersion

	app.Commands = []*cli.Command{
		{
			Name:  "generate",
			Usage: "Generate flaky test reports from GitHub Actions",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "repository",
					Value: defaultRepository,
					Usage: "GitHub repository (owner/repo)",
				},
				&cli.StringFlag{
					Name:  "branch",
					Value: defaultBranch,
					Usage: "Git branch to analyze",
				},
				&cli.IntFlag{
					Name:  "days",
					Value: defaultLookbackDays,
					Usage: "Number of days to look back",
				},
				&cli.Int64Flag{
					Name:  "workflow-id",
					Value: defaultWorkflowID,
					Usage: "GitHub Actions workflow ID",
				},
				&cli.IntFlag{
					Name:  "max-links",
					Value: defaultMaxLinks,
					Usage: "Maximum failure links per test",
				},
				&cli.IntFlag{
					Name:  "concurrency",
					Value: defaultConcurrency,
					Usage: "Number of parallel workers for artifact processing",
				},
				&cli.StringFlag{
					Name:  "output-dir",
					Value: "out",
					Usage: "Output directory for report files",
				},
				&cli.StringFlag{
					Name:    "slack-webhook",
					Usage:   "Slack webhook URL for notifications",
					EnvVars: []string{"SLACK_WEBHOOK"},
				},
				&cli.StringFlag{
					Name:  "run-id",
					Usage: "GitHub Actions run ID (for links)",
				},
				&cli.StringFlag{
					Name:  "ref-name",
					Usage: "Git ref name (for failure messages)",
				},
				&cli.StringFlag{
					Name:  "sha",
					Usage: "Git commit SHA (for failure messages)",
				},
			},
			Action: runGenerateCommand,
		},
	}

	return app
}

// fetchAndAnalyzeWorkflowRuns fetches workflow runs and counts successes
func fetchAndAnalyzeWorkflowRuns(ctx context.Context, repo string, workflowID int64, branch string, days int) ([]WorkflowRun, int, error) {
	fmt.Println("\n=== Fetching workflow runs ===")
	runs, err := fetchWorkflowRuns(ctx, repo, workflowID, branch, days)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch workflow runs: %w", err)
	}

	if len(runs) == 0 {
		fmt.Println("No workflow runs found in the specified time range")
		return nil, 0, nil
	}

	// Count successful runs
	successfulRuns := 0
	for _, run := range runs {
		if run.Conclusion == "success" {
			successfulRuns++
		}
	}
	fmt.Printf("Workflow runs: %d total, %d successful (%.1f%% success rate)\n",
		len(runs), successfulRuns, float64(successfulRuns)/float64(len(runs))*100.0)

	return runs, successfulRuns, nil
}

// collectArtifactJobs collects all artifact jobs from workflow runs
func collectArtifactJobs(ctx context.Context, repo string, runs []WorkflowRun, tempDir string) ([]ArtifactJob, error) {
	fmt.Println("\n=== Collecting artifacts ===")
	var jobs []ArtifactJob
	totalArtifacts := 0

	for i, run := range runs {
		artifacts, err := fetchRunArtifacts(ctx, repo, run.ID)
		if err != nil {
			fmt.Printf("Warning: Failed to fetch artifacts for run %d: %v\n", run.ID, err)
			continue
		}

		if len(artifacts) == 0 {
			fmt.Printf("Run %d/%d (ID: %d, CreatedAt: %s): No test artifacts found\n",
				i+1, len(runs), run.ID, run.CreatedAt.Format(time.RFC3339Nano))
			continue
		}

		fmt.Printf("Run %d/%d (ID: %d, CreatedAt: %s): Found %d artifacts\n",
			i+1, len(runs), run.ID, run.CreatedAt.Format(time.RFC3339Nano), len(artifacts))

		for _, artifact := range artifacts {
			totalArtifacts++
			jobs = append(jobs, ArtifactJob{
				Repo:         repo,
				RunID:        run.ID,
				RunCreatedAt: run.CreatedAt,
				Artifact:     artifact,
				TempDir:      tempDir,
				RunNumber:    i + 1,
				TotalRuns:    len(runs),
				ArtifactNum:  totalArtifacts,
			})
		}
	}

	fmt.Printf("\nTotal artifacts to process: %d\n", totalArtifacts)
	return jobs, nil
}

// buildReportSummary builds the complete report summary from processed data
func buildReportSummary(flakyReports, timeoutReports, crashReports, ciBreakerReports []TestReport,
	suiteReports []SuiteReport,
	allFailures []TestFailure, allTestRuns []TestRun, runs []WorkflowRun, successfulRuns int) *ReportSummary {

	// Calculate overall failure rate
	overallFailureRate := 0.0
	totalTestRuns := len(allTestRuns)
	if totalTestRuns > 0 {
		overallFailureRate = (float64(len(allFailures)) / float64(totalTestRuns)) * 1000.0
	}
	fmt.Printf("Overall failure rate: %.2f per 1000 test runs\n", overallFailureRate)

	return &ReportSummary{
		FlakyTests:         flakyReports,
		Timeouts:           timeoutReports,
		Crashes:            crashReports,
		CIBreakers:         ciBreakerReports,
		Suites:             suiteReports,
		TotalFailures:      len(allFailures),
		TotalTestRuns:      totalTestRuns,
		OverallFailureRate: overallFailureRate,
		TotalFlakyCount:    len(flakyReports),
		TotalWorkflowRuns:  len(runs),
		SuccessfulRuns:     successfulRuns,
	}
}

func runGenerateCommand(c *cli.Context) (err error) {
	// Extract parameters
	repo := c.String("repository")
	branch := c.String("branch")
	days := c.Int("days")
	workflowID := c.Int64("workflow-id")
	maxLinks := c.Int("max-links")
	concurrency := c.Int("concurrency")
	outputDir := c.String("output-dir")
	slackWebhook := c.String("slack-webhook")
	runID := c.String("run-id")
	refName := c.String("ref-name")
	sha := c.String("sha")

	// Send failure notification on error
	defer func() {
		if err != nil {
			sendFailureNotification(slackWebhook, runID, refName, sha, repo, err)
		}
	}()

	fmt.Println("Starting flaky test report generation...")
	fmt.Printf("Repository: %s\n", repo)
	fmt.Printf("Branch: %s\n", branch)
	fmt.Printf("Lookback days: %d\n", days)
	fmt.Printf("Workflow ID: %d\n", workflowID)
	fmt.Printf("Parallel workers: %d\n", concurrency)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Fetch and analyze workflow runs
	runs, successfulRuns, err := fetchAndAnalyzeWorkflowRuns(ctx, repo, workflowID, branch, days)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		return nil
	}

	// Create temp directory for downloads
	tempDir, err := os.MkdirTemp("", "flakereport-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			fmt.Printf("Warning: Failed to remove temp directory: %v\n", err)
		}
	}()

	// Collect all artifacts
	jobs, err := collectArtifactJobs(ctx, repo, runs, tempDir)
	if err != nil {
		return err
	}

	// Process artifacts in parallel
	fmt.Println("\n=== Processing artifacts in parallel ===")
	allFailures, allTestRuns, processedArtifacts := processArtifactsParallel(ctx, jobs, concurrency)

	fmt.Println("\n=== Processing Results ===")
	fmt.Printf("Total test runs: %d\n", len(allTestRuns))
	fmt.Printf("Total test failures found: %d\n", len(allFailures))
	fmt.Printf("Processed artifacts: %d\n", processedArtifacts)

	// Count test runs by name for failure rate calculation
	testRunCounts := countTestRuns(allTestRuns)

	// Group failures by test name
	grouped := groupFailuresByTest(allFailures)
	fmt.Printf("Unique tests with failures: %d\n", len(grouped))

	// Classify failures
	flakyMap, timeoutMap, crashMap := classifyFailures(grouped)
	fmt.Printf("Flaky tests: %d\n", len(flakyMap))
	fmt.Printf("Timeout tests: %d\n", len(timeoutMap))
	fmt.Printf("Crash tests: %d\n", len(crashMap))

	// Identify CI breakers (tests that failed all retries in a single job)
	ciBreakerMap, ciBreakCounts := identifyCIBreakers(allFailures)
	fmt.Printf("CI breaker tests (failed all retries): %d\n", len(ciBreakerMap))

	// Convert to reports with failure rates and sort
	flakyReports := convertToReports(flakyMap, testRunCounts, repo, maxLinks)
	timeoutReports := convertToReports(timeoutMap, testRunCounts, repo, maxLinks)
	crashReports := convertToReports(crashMap, testRunCounts, repo, maxLinks)
	ciBreakerReports := convertCIBreakersToReports(ciBreakerMap, ciBreakCounts, repo, maxLinks)

	// Compute suite-level breakdown
	suiteReports := generateSuiteReports(allFailures, allTestRuns)
	fmt.Printf("Suites: %d\n", len(suiteReports))

	// Build summary
	summary := buildReportSummary(
		flakyReports, timeoutReports, crashReports, ciBreakerReports,
		suiteReports,
		allFailures, allTestRuns, runs, successfulRuns,
	)

	fmt.Println("\n=== Writing report files ===")
	if err = os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Write failure JSON data
	if err = writeFailuresJSON(outputDir, allFailures, repo); err != nil {
		return fmt.Errorf("failed to write failures.json: %w", err)
	}

	// Write GitHub summary (to GITHUB_STEP_SUMMARY if set, otherwise to output dir)
	fmt.Println("\n=== Writing GitHub summary ===")
	summaryContent := generateGitHubSummary(summary, runID, maxLinks)
	if err := writeGitHubSummary(summaryContent, outputDir); err != nil {
		fmt.Printf("Warning: Failed to write GitHub summary: %v\n", err)
	}

	// Build and send Slack message
	message := buildSuccessMessage(summary, runID, repo, days)
	if slackWebhook != "" {
		fmt.Println("\n=== Sending Slack notification ===")
		if err := message.send(slackWebhook); err != nil {
			fmt.Printf("Warning: Failed to send Slack notification: %v\n", err)
		}
	} else {
		md := message.renderMarkdown()
		if writeErr := os.WriteFile(filepath.Join(outputDir, "slack-report.md"), []byte(md), 0644); writeErr != nil {
			fmt.Printf("Warning: Failed to write slack-report.md: %v\n", writeErr)
		}
	}

	fmt.Println("\n=== Report generation complete! ===")
	return nil
}

func sendFailureNotification(webhookURL, runID, refName, sha, repo string, err error) {
	if webhookURL == "" {
		return
	}

	fmt.Println("Sending failure notification to Slack...")
	message := buildFailureMessage(runID, refName, sha, repo)
	if sendErr := message.send(webhookURL); sendErr != nil {
		fmt.Printf("Warning: Failed to send failure notification: %v\n", sendErr)
	}
}

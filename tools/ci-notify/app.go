package cinotify

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"
	"go.uber.org/zap"
)

const (
	FlagRunID        = "run-id"
	FlagSlackWebhook = "slack-webhook"
	FlagDryRun       = "dry-run"
)

// NewCliApp instantiates a new instance of the CLI application
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "ci-notify"
	app.Usage = "Send Slack notifications for CI failures on main branch"
	app.Version = headers.ServerVersion

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     FlagRunID,
			Usage:    "GitHub Actions run ID",
			Required: true,
			EnvVars:  []string{"GITHUB_RUN_ID"},
		},
		&cli.StringFlag{
			Name:     FlagSlackWebhook,
			Usage:    "Slack webhook URL",
			Required: false, // Not required for dry-run
			EnvVars:  []string{"SLACK_WEBHOOK"},
		},
		&cli.BoolFlag{
			Name:  FlagDryRun,
			Usage: "Print message without sending to Slack",
			Value: false,
		},
	}

	app.Action = func(c *cli.Context) error {
		return run(c)
	}

	return app
}

func run(c *cli.Context) error {
	// Set up logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		return nil // Don't fail CI if notification fails
	}
	defer func() { _ = logger.Sync() }()

	runID := c.String(FlagRunID)
	slackWebhook := c.String(FlagSlackWebhook)
	dryRun := c.Bool(FlagDryRun)

	logger.Info("Starting ci-notify",
		zap.String("run_id", runID),
		zap.Bool("dry_run", dryRun),
	)

	// Build failure report
	report, err := BuildFailureReport(runID)
	if err != nil {
		logger.Error("Failed to build failure report", zap.Error(err))
		// Don't fail CI if notification fails
		return nil
	}

	logger.Info("Built failure report",
		zap.String("workflow", report.Workflow.Name),
		zap.String("sha", report.Commit.ShortSHA),
		zap.String("author", report.Commit.Author),
		zap.Int("failed_jobs", len(report.FailedJobs)),
		zap.Int("total_jobs", report.TotalJobs),
	)

	// Handle dry-run mode
	if dryRun {
		logger.Info("Dry-run mode: printing message to stdout")
		fmt.Println(FormatMessageForDebug(report))
		fmt.Println("\n--- Slack JSON Payload ---")
		message := BuildFailureMessage(report)
		payload, err := marshalIndent(message)
		if err != nil {
			logger.Error("Failed to marshal message for display", zap.Error(err))
			return nil
		}
		fmt.Println(payload)
		return nil
	}

	// Validate webhook URL
	if slackWebhook == "" {
		logger.Error("Slack webhook URL is required when not in dry-run mode")
		// Don't fail CI if notification fails
		return nil
	}

	// Build and send Slack message
	message := BuildFailureMessage(report)

	logger.Info("Sending Slack notification")
	if err := SendSlackMessage(slackWebhook, message); err != nil {
		logger.Error("Failed to send Slack message", zap.Error(err))
		// Don't fail CI if notification fails
		return nil
	}

	logger.Info("Slack notification sent successfully")
	return nil
}

func marshalIndent(v interface{}) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

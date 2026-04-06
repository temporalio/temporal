package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type WorkspaceAgentSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkspaceAgentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkspaceAgentSuite))
}

func (s *WorkspaceAgentSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)
}

// ReviewResult holds findings from a review sub-agent.
type ReviewResult struct {
	Agent    string   `json:"agent"`
	Findings []string `json:"findings"`
}

// FixInput carries the review findings to the fix agent.
type FixInput struct {
	WorkspaceID string         `json:"workspace_id"`
	Reviews     []ReviewResult `json:"reviews"`
}

type agentChildInput struct {
	WorkspaceID string `json:"workspace_id"`
}

// TestWorkspaceAgent_CodeReview validates the full multi-agent code review
// workflow exercising Fork (×2 concurrent sub-agents) and Handoff (fix agent).
//
// Flow:
//  1. SetupCodebase: write 3 source files
//  2. Fork → SecurityReviewAgent: scan for vulnerabilities (concurrent)
//  3. Fork → PerformanceReviewAgent: scan for perf issues (concurrent)
//  4. WriteSummary: combine findings into review-summary.txt
//  5. Handoff → FixAgent: apply fixes (replace MD5→SHA256, write marker)
//  6. ReadFinalState: verify summary + fixes + auth.go fixed
func (s *WorkspaceAgentSuite) TestWorkspaceAgent_CodeReview() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ws-agent-tq")
	workflowID := testcore.RandomizeStr("ws-agent-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	wsID := workflowID + "-code"

	// --- Parent Workflow ---
	codeReviewAgent := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
		}
		actCtx := workflow.WithActivityOptions(ctx, ao)

		// Step 1: Setup codebase.
		if err := workflow.ExecuteActivity(actCtx, "SetupCodebase").Get(ctx, nil); err != nil {
			return "", fmt.Errorf("SetupCodebase: %w", err)
		}

		// Step 2: Fork to review sub-agents (concurrent).
		secCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{SourceWorkspaceID: wsID, Mode: enumspb.WORKSPACE_TRANSFER_MODE_FORK},
			},
		})
		secFuture := workflow.ExecuteChildWorkflow(secCtx, "SecurityReviewAgent", agentChildInput{WorkspaceID: wsID})

		perfCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{SourceWorkspaceID: wsID, Mode: enumspb.WORKSPACE_TRANSFER_MODE_FORK},
			},
		})
		perfFuture := workflow.ExecuteChildWorkflow(perfCtx, "PerformanceReviewAgent", agentChildInput{WorkspaceID: wsID})

		var secResult, perfResult ReviewResult
		if err := secFuture.Get(ctx, &secResult); err != nil {
			return "", fmt.Errorf("SecurityReviewAgent: %w", err)
		}
		if err := perfFuture.Get(ctx, &perfResult); err != nil {
			return "", fmt.Errorf("PerformanceReviewAgent: %w", err)
		}

		// Step 3: Write review summary.
		if err := workflow.ExecuteActivity(actCtx, "WriteSummary", []ReviewResult{secResult, perfResult}).Get(ctx, nil); err != nil {
			return "", fmt.Errorf("WriteSummary: %w", err)
		}

		// Step 4: Handoff to fix agent.
		fixCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{SourceWorkspaceID: wsID, Mode: enumspb.WORKSPACE_TRANSFER_MODE_HANDOFF},
			},
		})
		if err := workflow.ExecuteChildWorkflow(fixCtx, "FixAgent", FixInput{
			WorkspaceID: wsID,
			Reviews:     []ReviewResult{secResult, perfResult},
		}).Get(ctx, nil); err != nil {
			return "", fmt.Errorf("FixAgent: %w", err)
		}

		// Step 5: Re-acquire workspace and read final state.
		var result string
		if err := workflow.ExecuteActivity(actCtx, "ReadFinalState").Get(ctx, &result); err != nil {
			return "", fmt.Errorf("ReadFinalState: %w", err)
		}
		return result, nil
	}

	// --- Child Workflows ---
	securityReviewAgent := func(ctx workflow.Context, input agentChildInput) (ReviewResult, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		var result ReviewResult
		err := workflow.ExecuteActivity(ctx, "AnalyzeSecurity").Get(ctx, &result)
		return result, err
	}

	perfReviewAgent := func(ctx workflow.Context, input agentChildInput) (ReviewResult, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		var result ReviewResult
		err := workflow.ExecuteActivity(ctx, "AnalyzePerformance").Get(ctx, &result)
		return result, err
	}

	fixAgent := func(ctx workflow.Context, input FixInput) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return workflow.ExecuteActivity(ctx, "ApplyFixes", input.Reviews).Get(ctx, nil)
	}

	// --- Activities ---
	setupCodebase := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(wsPath, "src"), 0o755); err != nil {
			return err
		}
		files := map[string]string{
			"src/main.go": "package main\nfunc main() {\n\tpassword := \"admin123\"\n}\n",
			"src/api.go":  "package main\nimport \"net/http\"\nfunc handle(w http.ResponseWriter, r *http.Request) {\n\tfor _, id := range ids { db.Query(\"SELECT * FROM users WHERE id = ?\", id) }\n}\n",
			"src/auth.go": "package main\nimport \"crypto/md5\"\nfunc hash(p string) []byte {\n\th := md5.Sum([]byte(p)) // weak hash\n\treturn h[:]\n}\n",
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(wsPath, name), []byte(content), 0o644); err != nil {
				return err
			}
		}
		return nil
	}

	analyzeSecurity := func(ctx context.Context) (ReviewResult, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return ReviewResult{}, err
		}
		var findings []string
		entries, _ := os.ReadDir(filepath.Join(wsPath, "src"))
		for _, e := range entries {
			data, _ := os.ReadFile(filepath.Join(wsPath, "src", e.Name()))
			c := string(data)
			if strings.Contains(c, "password") {
				findings = append(findings, e.Name()+": hardcoded credential")
			}
			if strings.Contains(c, "md5") {
				findings = append(findings, e.Name()+": weak hash (MD5)")
			}
		}
		return ReviewResult{Agent: "security", Findings: findings}, nil
	}

	analyzePerformance := func(ctx context.Context) (ReviewResult, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return ReviewResult{}, err
		}
		var findings []string
		entries, _ := os.ReadDir(filepath.Join(wsPath, "src"))
		for _, e := range entries {
			data, _ := os.ReadFile(filepath.Join(wsPath, "src", e.Name()))
			if strings.Contains(string(data), "db.Query") && strings.Contains(string(data), "for") {
				findings = append(findings, e.Name()+": N+1 query")
			}
		}
		return ReviewResult{Agent: "performance", Findings: findings}, nil
	}

	writeSummary := func(ctx context.Context, reviews []ReviewResult) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		var sb strings.Builder
		for _, r := range reviews {
			sb.WriteString(r.Agent + ":")
			sb.WriteString(strings.Join(r.Findings, ";"))
			sb.WriteString("\n")
		}
		return os.WriteFile(filepath.Join(wsPath, "review-summary.txt"), []byte(sb.String()), 0o644)
	}

	applyFixes := func(ctx context.Context, _ []ReviewResult) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		// Verify review summary is visible (written before handoff).
		if _, err := os.Stat(filepath.Join(wsPath, "review-summary.txt")); err != nil {
			return fmt.Errorf("review-summary.txt missing: %w", err)
		}
		// Fix auth.go: MD5 → SHA256.
		authPath := filepath.Join(wsPath, "src", "auth.go")
		data, err := os.ReadFile(authPath)
		if err != nil {
			return err
		}
		fixed := strings.ReplaceAll(string(data), "crypto/md5", "crypto/sha256")
		fixed = strings.ReplaceAll(fixed, "md5.Sum", "sha256.Sum256")
		if err := os.WriteFile(authPath, []byte(fixed), 0o644); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "fixes-applied.txt"), []byte("auth.go:MD5→SHA256"), 0o644)
	}

	readFinalState := func(ctx context.Context) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", err
		}
		var parts []string
		if _, err := os.Stat(filepath.Join(wsPath, "review-summary.txt")); err == nil {
			parts = append(parts, "summary:present")
		} else {
			parts = append(parts, "summary:missing")
		}
		if data, err := os.ReadFile(filepath.Join(wsPath, "fixes-applied.txt")); err == nil {
			parts = append(parts, "fixes:"+strings.TrimSpace(string(data)))
		} else {
			parts = append(parts, "fixes:missing")
		}
		if data, err := os.ReadFile(filepath.Join(wsPath, "src", "auth.go")); err == nil {
			if strings.Contains(string(data), "sha256") {
				parts = append(parts, "auth:fixed")
			} else {
				parts = append(parts, "auth:unfixed")
			}
		}
		return strings.Join(parts, "|"), nil
	}

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	driver := converter.NewLocalFileStoreDriver(storeDir)
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{WorkspaceManager: wsMgr})
	w.RegisterWorkflowWithOptions(codeReviewAgent, workflow.RegisterOptions{Name: "CodeReviewAgent"})
	w.RegisterWorkflowWithOptions(securityReviewAgent, workflow.RegisterOptions{Name: "SecurityReviewAgent"})
	w.RegisterWorkflowWithOptions(perfReviewAgent, workflow.RegisterOptions{Name: "PerformanceReviewAgent"})
	w.RegisterWorkflowWithOptions(fixAgent, workflow.RegisterOptions{Name: "FixAgent"})
	w.RegisterActivityWithOptions(setupCodebase, activity.RegisterOptions{Name: "SetupCodebase"})
	w.RegisterActivityWithOptions(analyzeSecurity, activity.RegisterOptions{Name: "AnalyzeSecurity"})
	w.RegisterActivityWithOptions(analyzePerformance, activity.RegisterOptions{Name: "AnalyzePerformance"})
	w.RegisterActivityWithOptions(writeSummary, activity.RegisterOptions{Name: "WriteSummary"})
	w.RegisterActivityWithOptions(applyFixes, activity.RegisterOptions{Name: "ApplyFixes"})
	w.RegisterActivityWithOptions(readFinalState, activity.RegisterOptions{Name: "ReadFinalState"})
	s.NoError(w.Start())
	defer w.Stop()

	// --- Execute ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "CodeReviewAgent")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))

	// Verify: review summary present, fixes applied, auth.go fixed.
	s.Contains(result, "summary:present")
	s.Contains(result, "fixes:auth.go:MD5→SHA256")
	s.Contains(result, "auth:fixed")
}

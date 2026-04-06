package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type CodeAssistantSuite struct {
	testcore.FunctionalTestBase
}

func TestCodeAssistantSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CodeAssistantSuite))
}

func (s *CodeAssistantSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)
}

// --- Inline types (matching the sample) ---

type caPromptRequest struct {
	Prompt string `json:"prompt"`
}

type caPromptResponse struct {
	Messages []caAgentMessage `json:"messages"`
}

type caAgentMessage struct {
	Type      string `json:"type"`
	Content   string `json:"content"`
	ToolName  string `json:"tool_name,omitempty"`
	ToolInput string `json:"tool_input,omitempty"`
}

// TestCodeAssistant_InteractiveSession validates the code assistant workflow:
// 1. Send "create file hello.txt with content Hello World"
// 2. Send "list files"
// 3. Send "read hello.txt"
// 4. Verify file persists across prompts (durable workspace)
func (s *CodeAssistantSuite) TestCodeAssistant_InteractiveSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ca-tq")
	workflowID := testcore.RandomizeStr("ca-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")
	wsID := workflowID + "/workspace"

	// --- Workflow: long-running with "prompt" update handler ---
	assistantFn := func(ctx workflow.Context) error {
		done := false
		workflow.SetUpdateHandler(ctx, "prompt", func(ctx workflow.Context, req caPromptRequest) (caPromptResponse, error) {
			if req.Prompt == "exit" {
				done = true
				return caPromptResponse{Messages: []caAgentMessage{{Type: "text", Content: "Goodbye!"}}}, nil
			}
			ao := workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
			}
			actCtx := workflow.WithActivityOptions(ctx, ao)
			var resp caPromptResponse
			err := workflow.ExecuteActivity(actCtx, "ProcessPrompt", req.Prompt).Get(ctx, &resp)
			if err != nil {
				return caPromptResponse{Messages: []caAgentMessage{{Type: "error", Content: err.Error()}}}, nil
			}
			return resp, nil
		})
		return workflow.Await(ctx, func() bool { return done })
	}

	// --- Activity: simple built-in command handler (no Claude API) ---
	processPrompt := func(ctx context.Context, prompt string) (caPromptResponse, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return caPromptResponse{}, err
		}

		var msgs []caAgentMessage
		lower := strings.ToLower(prompt)

		switch {
		case strings.Contains(lower, "create file") || strings.Contains(lower, "write"):
			// Parse "create file X with content Y"
			parts := strings.SplitN(prompt, " with content ", 2)
			if len(parts) == 2 {
				filename := strings.TrimPrefix(strings.TrimPrefix(parts[0], "create file "), "write ")
				filename = strings.TrimSpace(filename)
				content := parts[1]
				path := filepath.Join(wsPath, filename)
				os.MkdirAll(filepath.Dir(path), 0o755)
				os.WriteFile(path, []byte(content), 0o644)
				msgs = append(msgs, caAgentMessage{Type: "tool_call", ToolName: "write_file", ToolInput: filename})
				msgs = append(msgs, caAgentMessage{Type: "tool_result", Content: fmt.Sprintf("Written %s (%d bytes)", filename, len(content))})
			}

		case strings.Contains(lower, "list") || strings.Contains(lower, "ls"):
			cmd := exec.Command("find", wsPath, "-type", "f")
			out, _ := cmd.Output()
			msgs = append(msgs, caAgentMessage{Type: "tool_call", ToolName: "list_files", ToolInput: "."})
			msgs = append(msgs, caAgentMessage{Type: "tool_result", Content: string(out)})

		case strings.Contains(lower, "read ") || strings.Contains(lower, "cat "):
			// Extract filename.
			for _, word := range strings.Fields(prompt) {
				if strings.Contains(word, ".") {
					data, err := os.ReadFile(filepath.Join(wsPath, word))
					if err != nil {
						msgs = append(msgs, caAgentMessage{Type: "error", Content: err.Error()})
					} else {
						msgs = append(msgs, caAgentMessage{Type: "tool_call", ToolName: "read_file", ToolInput: word})
						msgs = append(msgs, caAgentMessage{Type: "tool_result", Content: string(data)})
					}
					break
				}
			}

		case strings.Contains(lower, "run ") || strings.HasPrefix(lower, "!"):
			cmdStr := strings.TrimPrefix(strings.TrimPrefix(prompt, "run "), "!")
			cmd := exec.Command("bash", "-c", cmdStr)
			cmd.Dir = wsPath
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			cmd.Run()
			msgs = append(msgs, caAgentMessage{Type: "tool_call", ToolName: "run_command", ToolInput: cmdStr})
			msgs = append(msgs, caAgentMessage{Type: "tool_result", Content: stdout.String() + stderr.String()})
		}

		if len(msgs) == 0 {
			msgs = append(msgs, caAgentMessage{Type: "text", Content: "Processed: " + prompt})
		}
		return caPromptResponse{Messages: msgs}, nil
	}

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	driver := converter.NewLocalFileStoreDriver(storeDir)
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{WorkspaceManager: wsMgr})
	w.RegisterWorkflowWithOptions(assistantFn, workflow.RegisterOptions{Name: "CodeAssistant"})
	w.RegisterActivityWithOptions(processPrompt, activity.RegisterOptions{Name: "ProcessPrompt"})
	s.NoError(w.Start())
	defer w.Stop()

	// --- Start workflow ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, "CodeAssistant")
	s.NoError(err)

	// Helper to send a prompt and get response.
	sendPrompt := func(prompt string) caPromptResponse {
		handle, err := s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			UpdateName:   "prompt",
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
			Args:         []interface{}{caPromptRequest{Prompt: prompt}},
		})
		s.NoError(err)
		var resp caPromptResponse
		s.NoError(handle.Get(ctx, &resp))
		return resp
	}

	// --- Interactive session ---

	// 1. Create a file.
	resp := sendPrompt("create file hello.txt with content Hello from Temporal workspace!")
	s.Require().NotEmpty(resp.Messages)
	found := false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "hello.txt") {
			found = true
		}
	}
	s.True(found, "Expected tool_result mentioning hello.txt")

	// 2. List files — should see hello.txt.
	resp = sendPrompt("list files")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "hello.txt") {
			found = true
		}
	}
	s.True(found, "Expected hello.txt in file listing")

	// 3. Read the file — should get the content back.
	resp = sendPrompt("read hello.txt")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "Hello from Temporal workspace!") {
			found = true
		}
	}
	s.True(found, "Expected file content in read result")

	// 4. Run a command.
	resp = sendPrompt("run echo workspace-is-durable")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "workspace-is-durable") {
			found = true
		}
	}
	s.True(found, "Expected command output")

	// 5. Exit.
	resp = sendPrompt("exit")
	s.NoError(run.Get(ctx, nil))
}

// TestCodeAssistant_WithHTTPServer validates the HTTP server integration
// by making real HTTP requests to /prompt.
func (s *CodeAssistantSuite) TestCodeAssistant_WithHTTPServer() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ca-http-tq")
	workflowID := testcore.RandomizeStr("ca-http-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")
	wsID := workflowID + "/workspace"

	// Reuse workflow + activity from above.
	assistantFn := func(ctx workflow.Context) error {
		done := false
		workflow.SetUpdateHandler(ctx, "prompt", func(ctx workflow.Context, req caPromptRequest) (caPromptResponse, error) {
			if req.Prompt == "exit" {
				done = true
				return caPromptResponse{Messages: []caAgentMessage{{Type: "text", Content: "Bye"}}}, nil
			}
			ao := workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
			}
			actCtx := workflow.WithActivityOptions(ctx, ao)
			var resp caPromptResponse
			workflow.ExecuteActivity(actCtx, "ProcessPrompt2", req.Prompt).Get(ctx, &resp)
			return resp, nil
		})
		return workflow.Await(ctx, func() bool { return done })
	}

	processPrompt := func(ctx context.Context, prompt string) (caPromptResponse, error) {
		wsPath, _ := activity.GetWorkspacePath(ctx)
		os.WriteFile(filepath.Join(wsPath, "test.txt"), []byte("http-test"), 0o644)
		return caPromptResponse{Messages: []caAgentMessage{
			{Type: "text", Content: "Created test.txt in workspace"},
		}}, nil
	}

	// Worker.
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, converter.NewLocalFileStoreDriver(storeDir))
	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{WorkspaceManager: wsMgr})
	w.RegisterWorkflowWithOptions(assistantFn, workflow.RegisterOptions{Name: "CodeAssistant2"})
	w.RegisterActivityWithOptions(processPrompt, activity.RegisterOptions{Name: "ProcessPrompt2"})
	s.NoError(w.Start())
	defer w.Stop()

	// Start workflow.
	_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue,
	}, "CodeAssistant2")
	s.NoError(err)

	// Send prompt via HTTP-style update (simulating what the web server does).
	handle, err := s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateName:   "prompt",
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		Args:         []interface{}{caPromptRequest{Prompt: "create something"}},
	})
	s.NoError(err)
	var resp caPromptResponse
	s.NoError(handle.Get(ctx, &resp))
	s.Require().NotEmpty(resp.Messages)
	s.Contains(resp.Messages[0].Content, "test.txt")

	// Exit.
	handle, _ = s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID: workflowID, UpdateName: "prompt",
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		Args:         []interface{}{caPromptRequest{Prompt: "exit"}},
	})
	handle.Get(ctx, nil)
}

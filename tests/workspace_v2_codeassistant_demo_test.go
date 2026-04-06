package tests

import (
	"context"
	"encoding/json"
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

type CodeAssistantDemoSuite struct {
	testcore.FunctionalTestBase
}

func TestCodeAssistantDemoSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CodeAssistantDemoSuite))
}

func (s *CodeAssistantDemoSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)
}

type demoPromptRequest struct {
	Prompt string `json:"prompt"`
}

type demoPromptResponse struct {
	Messages []demoAgentMessage `json:"messages"`
}

type demoAgentMessage struct {
	Type      string `json:"type"`
	Content   string `json:"content"`
	ToolName  string `json:"tool_name,omitempty"`
	ToolInput string `json:"tool_input,omitempty"`
}

// TestCodeAssistantDemo_FullSession runs a multi-step interactive coding session
// that exercises workspace durability, file manipulation, and command execution.
// This simulates what a user would see in the web UI.
func (s *CodeAssistantDemoSuite) TestCodeAssistantDemo_FullSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("demo-tq")
	workflowID := testcore.RandomizeStr("demo-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")
	wsID := workflowID + "/workspace"

	// --- Workflow ---
	assistantFn := func(ctx workflow.Context) error {
		done := false
		workflow.SetUpdateHandler(ctx, "prompt", func(ctx workflow.Context, req demoPromptRequest) (demoPromptResponse, error) {
			if req.Prompt == "exit" {
				done = true
				return demoPromptResponse{Messages: []demoAgentMessage{{Type: "text", Content: "Goodbye!"}}}, nil
			}
			ao := workflow.ActivityOptions{
				StartToCloseTimeout: 60 * time.Second,
				WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
			}
			actCtx := workflow.WithActivityOptions(ctx, ao)
			var resp demoPromptResponse
			workflow.ExecuteActivity(actCtx, "DemoProcess", req.Prompt).Get(ctx, &resp)
			return resp, nil
		})
		return workflow.Await(ctx, func() bool { return done })
	}

	// --- Activity: built-in command handler ---
	processPrompt := func(ctx context.Context, prompt string) (demoPromptResponse, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return demoPromptResponse{}, err
		}

		var msgs []demoAgentMessage
		lower := strings.ToLower(prompt)

		switch {
		case strings.Contains(lower, "create") && strings.Contains(lower, "with content"):
			parts := strings.SplitN(prompt, " with content ", 2)
			if len(parts) == 2 {
				// Extract filename from "create file X"
				filename := strings.TrimSpace(parts[0])
				for _, prefix := range []string{"create file ", "create ", "write file ", "write "} {
					filename = strings.TrimPrefix(filename, prefix)
				}
				content := parts[1]
				path := filepath.Join(wsPath, filename)
				os.MkdirAll(filepath.Dir(path), 0o755)
				os.WriteFile(path, []byte(content), 0o644)
				msgs = append(msgs, demoAgentMessage{Type: "tool_call", ToolName: "write_file", ToolInput: fmt.Sprintf(`{"path": "%s"}`, filename)})
				msgs = append(msgs, demoAgentMessage{Type: "tool_result", Content: fmt.Sprintf("Written %s (%d bytes)", filename, len(content))})
			}

		case strings.Contains(lower, "list"):
			out, _ := exec.Command("find", wsPath, "-type", "f", "-not", "-path", "*/.*").Output()
			// Make paths relative
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			var relative []string
			for _, l := range lines {
				if rel, err := filepath.Rel(wsPath, l); err == nil && rel != "." {
					relative = append(relative, rel)
				}
			}
			msgs = append(msgs, demoAgentMessage{Type: "tool_call", ToolName: "list_files", ToolInput: `{"path": "."}`})
			msgs = append(msgs, demoAgentMessage{Type: "tool_result", Content: strings.Join(relative, "\n")})

		case strings.Contains(lower, "read ") || strings.Contains(lower, "cat "):
			for _, word := range strings.Fields(prompt) {
				if strings.Contains(word, ".") || strings.Contains(word, "/") {
					data, err := os.ReadFile(filepath.Join(wsPath, word))
					if err != nil {
						msgs = append(msgs, demoAgentMessage{Type: "error", Content: err.Error()})
					} else {
						msgs = append(msgs, demoAgentMessage{Type: "tool_call", ToolName: "read_file", ToolInput: fmt.Sprintf(`{"path": "%s"}`, word)})
						msgs = append(msgs, demoAgentMessage{Type: "tool_result", Content: string(data)})
					}
					break
				}
			}

		case strings.HasPrefix(lower, "run ") || strings.HasPrefix(lower, "!"):
			cmdStr := prompt
			if strings.HasPrefix(lower, "run ") {
				cmdStr = prompt[4:]
			} else if strings.HasPrefix(cmdStr, "!") {
				cmdStr = cmdStr[1:]
			}
			cmd := exec.Command("bash", "-c", cmdStr)
			cmd.Dir = wsPath
			out, err := cmd.CombinedOutput()
			result := string(out)
			if err != nil {
				result += "\n[exit: " + err.Error() + "]"
			}
			msgs = append(msgs, demoAgentMessage{Type: "tool_call", ToolName: "run_command", ToolInput: fmt.Sprintf(`{"command": "%s"}`, cmdStr)})
			msgs = append(msgs, demoAgentMessage{Type: "tool_result", Content: result})

		default:
			msgs = append(msgs, demoAgentMessage{Type: "text", Content: "Processed: " + prompt})
		}

		return demoPromptResponse{Messages: msgs}, nil
	}

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, converter.NewLocalFileStoreDriver(storeDir))

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{WorkspaceManager: wsMgr})
	w.RegisterWorkflowWithOptions(assistantFn, workflow.RegisterOptions{Name: "DemoAssistant"})
	w.RegisterActivityWithOptions(processPrompt, activity.RegisterOptions{Name: "DemoProcess"})
	s.NoError(w.Start())
	defer w.Stop()

	// --- Start workflow ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue,
	}, "DemoAssistant")
	s.NoError(err)

	// --- Helper ---
	sendPrompt := func(prompt string) demoPromptResponse {
		s.T().Logf("\n💬 User: %s", prompt)
		handle, err := s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			UpdateName:   "prompt",
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
			Args:         []interface{}{demoPromptRequest{Prompt: prompt}},
		})
		s.NoError(err)
		var resp demoPromptResponse
		s.NoError(handle.Get(ctx, &resp))

		for _, m := range resp.Messages {
			switch m.Type {
			case "text":
				s.T().Logf("🤖 %s", m.Content)
			case "tool_call":
				s.T().Logf("⚡ %s(%s)", m.ToolName, m.ToolInput)
			case "tool_result":
				lines := strings.Split(m.Content, "\n")
				if len(lines) > 10 {
					s.T().Logf("📄 %s\n   ... (%d more lines)", strings.Join(lines[:10], "\n   "), len(lines)-10)
				} else {
					s.T().Logf("📄 %s", m.Content)
				}
			case "error":
				s.T().Logf("❌ %s", m.Content)
			}
		}
		return resp
	}

	// --- Interactive demo session ---
	s.T().Log("\n========== AI Code Assistant Demo ==========\n")

	// Step 1: Create a Go project
	resp := sendPrompt("create file main.go with content package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"Hello from Temporal workspace!\")\n}")
	s.Require().NotEmpty(resp.Messages)

	// Step 2: Create a test file
	sendPrompt("create file main_test.go with content package main\n\nimport \"testing\"\n\nfunc TestHello(t *testing.T) {\n\tt.Log(\"test passed\")\n}")

	// Step 3: Create go.mod
	sendPrompt("create file go.mod with content module example.com/demo\n\ngo 1.21")

	// Step 4: List files
	resp = sendPrompt("list files")
	found := false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "main.go") {
			found = true
		}
	}
	s.True(found, "Expected main.go in listing")

	// Step 5: Read back the file
	resp = sendPrompt("read main.go")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "Hello from Temporal workspace") {
			found = true
		}
	}
	s.True(found, "Expected file content")

	// Step 6: Run go build
	resp = sendPrompt("run go build -o /dev/null .")
	s.T().Log("")

	// Step 7: Run the tests
	resp = sendPrompt("run go test -v ./...")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "PASS") {
			found = true
		}
	}
	s.True(found, "Expected tests to pass")

	// Step 8: Modify a file
	sendPrompt("create file main.go with content package main\n\nimport \"fmt\"\n\nfunc Greet(name string) string {\n\treturn fmt.Sprintf(\"Hello, %s!\", name)\n}\n\nfunc main() {\n\tfmt.Println(Greet(\"Temporal\"))\n}")

	// Step 9: Read modified file
	resp = sendPrompt("read main.go")
	found = false
	for _, m := range resp.Messages {
		if m.Type == "tool_result" && strings.Contains(m.Content, "Greet") {
			found = true
		}
	}
	s.True(found, "Expected modified file with Greet function")

	// Step 10: Run tree
	sendPrompt("run find . -type f | sort")

	s.T().Log("\n========== Demo Complete ==========\n")

	// Print summary as JSON for clarity
	summary := map[string]interface{}{
		"workspace_id": wsID,
		"workflow_id":  workflowID,
		"prompts_sent": 10,
		"files_created": []string{"main.go", "main_test.go", "go.mod"},
		"commands_run":  []string{"go build", "go test", "find"},
	}
	summaryJSON, _ := json.MarshalIndent(summary, "", "  ")
	s.T().Logf("Session summary:\n%s", string(summaryJSON))

	// Exit
	sendPrompt("exit")
	s.NoError(run.Get(ctx, nil))
}

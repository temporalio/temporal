package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
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

type OpenAIDemoSuite struct {
	testcore.FunctionalTestBase
}

func TestOpenAIDemoSuite(t *testing.T) {
	// Load API keys from env or from .local/ files.
	loadEnvFile := func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		for _, line := range strings.Split(string(data), "\n") {
			if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				val := strings.TrimSpace(parts[1])
				if key != "" && os.Getenv(key) == "" {
					os.Setenv(key, val)
				}
			}
		}
	}
	loadEnvFile("/Users/yimin/code/samples-go/.local/ENV.claude")
	loadEnvFile("/Users/yimin/code/samples-go/.local/ENV.openai")

	if os.Getenv("ANTHROPIC_API_KEY") == "" && os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("No LLM API key found (set ANTHROPIC_API_KEY or OPENAI_API_KEY, or create .local/ENV.claude or .local/ENV.openai)")
	}
	t.Parallel()
	suite.Run(t, new(OpenAIDemoSuite))
}

func (s *OpenAIDemoSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)
}

// --- Inline types matching the codeassistant sample ---

type oaiAgentMessage struct {
	Type      string `json:"type"`
	Content   string `json:"content"`
	ToolName  string `json:"tool_name,omitempty"`
	ToolInput string `json:"tool_input,omitempty"`
}

type oaiSubAgentReq struct {
	AgentType    string `json:"agent_type"`
	Task         string `json:"task"`
	TransferMode string `json:"transfer_mode"`
}

type oaiProcessResult struct {
	Messages      []oaiAgentMessage `json:"messages"`
	NeedsSubAgent *oaiSubAgentReq   `json:"needs_sub_agent,omitempty"`
}

type oaiSubAgentInput struct {
	WorkspaceID  string `json:"workspace_id"`
	SystemPrompt string `json:"system_prompt"`
	Task         string `json:"task"`
}

type oaiPromptReq struct {
	Prompt string `json:"prompt"`
}

type oaiPromptResp struct {
	Messages []oaiAgentMessage `json:"messages"`
}

// --- Provider-agnostic LLM caller ---

func callLLM(ctx context.Context, system, prompt, wsPath string) (oaiProcessResult, error) {
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		return callClaude(ctx, key, system, prompt, wsPath)
	}
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		return callGPT(ctx, key, system, prompt, wsPath)
	}
	return oaiProcessResult{}, fmt.Errorf("no LLM API key set")
}

// --- Claude API caller ---

func callClaude(ctx context.Context, apiKey, system, prompt, wsPath string) (oaiProcessResult, error) {
	type contentBlock struct {
		Type      string                 `json:"type"`
		Text      string                 `json:"text,omitempty"`
		ID        string                 `json:"id,omitempty"`
		Name      string                 `json:"name,omitempty"`
		Input     map[string]interface{} `json:"input,omitempty"`
		ToolUseID string                 `json:"tool_use_id,omitempty"`
		Content   string                 `json:"content,omitempty"`
	}
	type msg struct {
		Role    string         `json:"role"`
		Content []contentBlock `json:"content"`
	}

	tools := []map[string]interface{}{
		{"name": "write_file", "description": "Write a file", "input_schema": map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"}, "content": map[string]interface{}{"type": "string"},
			}, "required": []string{"path", "content"},
		}},
		{"name": "read_file", "description": "Read a file", "input_schema": map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"},
			}, "required": []string{"path"},
		}},
		{"name": "run_command", "description": "Run a shell command", "input_schema": map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{
				"command": map[string]interface{}{"type": "string"},
			}, "required": []string{"command"},
		}},
		{"name": "list_files", "description": "List files", "input_schema": map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"},
			}, "required": []string{"path"},
		}},
		{"name": "delegate_to_subagent", "description": "Delegate task to a sub-agent", "input_schema": map[string]interface{}{
			"type": "object", "properties": map[string]interface{}{
				"agent_type":    map[string]interface{}{"type": "string", "enum": []string{"reviewer", "implementer", "tester"}},
				"task":          map[string]interface{}{"type": "string"},
				"transfer_mode": map[string]interface{}{"type": "string", "enum": []string{"fork", "handoff"}},
			}, "required": []string{"agent_type", "task"},
		}},
	}

	messages := []msg{{Role: "user", Content: []contentBlock{{Type: "text", Text: prompt}}}}
	var steps []oaiAgentMessage
	var subAgent *oaiSubAgentReq

	for i := 0; i < 15; i++ {
		body, _ := json.Marshal(map[string]interface{}{
			"model": "claude-sonnet-4-6", "max_tokens": 4096,
			"system": system + "\nWorkspace: " + wsPath,
			"messages": messages, "tools": tools,
		})
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")

		resp, err := http.DefaultClient.Do(req)
		if err != nil { return oaiProcessResult{}, err }
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return oaiProcessResult{}, fmt.Errorf("Claude %d: %s", resp.StatusCode, string(respBody))
		}

		var result struct {
			Content    []contentBlock `json:"content"`
			StopReason string         `json:"stop_reason"`
		}
		json.Unmarshal(respBody, &result)

		var toolResults []contentBlock
		delegated := false
		for _, b := range result.Content {
			switch b.Type {
			case "text":
				if b.Text != "" { steps = append(steps, oaiAgentMessage{Type: "text", Content: b.Text}) }
			case "tool_use":
				inputJSON, _ := json.Marshal(b.Input)
				steps = append(steps, oaiAgentMessage{Type: "tool_call", ToolName: b.Name, ToolInput: string(inputJSON)})

				if b.Name == "delegate_to_subagent" {
					at, _ := b.Input["agent_type"].(string)
					task, _ := b.Input["task"].(string)
					tm, _ := b.Input["transfer_mode"].(string)
					if tm == "" { if at == "implementer" { tm = "handoff" } else { tm = "fork" } }
					subAgent = &oaiSubAgentReq{AgentType: at, Task: task, TransferMode: tm}
					toolResults = append(toolResults, contentBlock{Type: "tool_result", ToolUseID: b.ID, Content: "Delegating to " + at})
					delegated = true
					continue
				}

				r := execToolLocal(wsPath, b.Name, b.Input)
				steps = append(steps, oaiAgentMessage{Type: "tool_result", Content: trunc(r, 3000)})
				toolResults = append(toolResults, contentBlock{Type: "tool_result", ToolUseID: b.ID, Content: trunc(r, 6000)})
			}
		}

		messages = append(messages, msg{Role: "assistant", Content: result.Content})
		if delegated || result.StopReason != "tool_use" { break }
		messages = append(messages, msg{Role: "user", Content: toolResults})
	}
	return oaiProcessResult{Messages: steps, NeedsSubAgent: subAgent}, nil
}

// --- OpenAI API caller ---

func callGPT(ctx context.Context, apiKey, system, prompt string, wsPath string) (oaiProcessResult, error) {
	tools := []map[string]interface{}{
		{"type": "function", "function": map[string]interface{}{
			"name": "write_file", "description": "Write a file",
			"parameters": map[string]interface{}{"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"}, "content": map[string]interface{}{"type": "string"},
			}, "required": []string{"path", "content"}},
		}},
		{"type": "function", "function": map[string]interface{}{
			"name": "read_file", "description": "Read a file",
			"parameters": map[string]interface{}{"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"},
			}, "required": []string{"path"}},
		}},
		{"type": "function", "function": map[string]interface{}{
			"name": "run_command", "description": "Run a shell command in workspace",
			"parameters": map[string]interface{}{"type": "object", "properties": map[string]interface{}{
				"command": map[string]interface{}{"type": "string"},
			}, "required": []string{"command"}},
		}},
		{"type": "function", "function": map[string]interface{}{
			"name": "list_files", "description": "List files",
			"parameters": map[string]interface{}{"type": "object", "properties": map[string]interface{}{
				"path": map[string]interface{}{"type": "string"},
			}, "required": []string{"path"}},
		}},
		{"type": "function", "function": map[string]interface{}{
			"name": "delegate_to_subagent", "description": "Delegate task to a sub-agent",
			"parameters": map[string]interface{}{"type": "object", "properties": map[string]interface{}{
				"agent_type":    map[string]interface{}{"type": "string", "enum": []string{"reviewer", "implementer", "tester"}},
				"task":          map[string]interface{}{"type": "string"},
				"transfer_mode": map[string]interface{}{"type": "string", "enum": []string{"fork", "handoff"}},
			}, "required": []string{"agent_type", "task"}},
		}},
	}

	messages := []map[string]interface{}{
		{"role": "system", "content": system + "\nWorkspace: " + wsPath},
		{"role": "user", "content": prompt},
	}

	var steps []oaiAgentMessage
	var subAgent *oaiSubAgentReq

	for i := 0; i < 15; i++ {
		body, _ := json.Marshal(map[string]interface{}{
			"model": "gpt-5.4-mini", "messages": messages, "tools": tools, "max_completion_tokens": 4096,
		})
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return oaiProcessResult{}, err
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return oaiProcessResult{}, fmt.Errorf("OpenAI %d: %s", resp.StatusCode, string(respBody))
		}

		var result struct {
			Choices []struct {
				Message struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"message"`
			} `json:"choices"`
		}
		json.Unmarshal(respBody, &result)

		if len(result.Choices) == 0 {
			break
		}
		choice := result.Choices[0]

		if choice.Message.Content != "" {
			steps = append(steps, oaiAgentMessage{Type: "text", Content: choice.Message.Content})
		}

		if len(choice.Message.ToolCalls) == 0 {
			break
		}

		// Build assistant message for history — include "type":"function" on each tool call.
		var tcList []map[string]interface{}
		for _, tc := range choice.Message.ToolCalls {
			tcList = append(tcList, map[string]interface{}{
				"id": tc.ID, "type": "function",
				"function": map[string]interface{}{
					"name": tc.Function.Name, "arguments": tc.Function.Arguments,
				},
			})
		}
		assistantMsg := map[string]interface{}{"role": "assistant", "tool_calls": tcList}
		if choice.Message.Content != "" {
			assistantMsg["content"] = choice.Message.Content
		}
		messages = append(messages, assistantMsg)

		delegated := false
		for _, tc := range choice.Message.ToolCalls {
			var input map[string]interface{}
			json.Unmarshal([]byte(tc.Function.Arguments), &input)

			steps = append(steps, oaiAgentMessage{Type: "tool_call", ToolName: tc.Function.Name, ToolInput: tc.Function.Arguments})

			if tc.Function.Name == "delegate_to_subagent" {
				at, _ := input["agent_type"].(string)
				task, _ := input["task"].(string)
				tm, _ := input["transfer_mode"].(string)
				if tm == "" {
					if at == "implementer" {
						tm = "handoff"
					} else {
						tm = "fork"
					}
				}
				subAgent = &oaiSubAgentReq{AgentType: at, Task: task, TransferMode: tm}
				messages = append(messages, map[string]interface{}{
					"role": "tool", "tool_call_id": tc.ID,
					"content": fmt.Sprintf("Delegating to %s sub-agent.", at),
				})
				delegated = true
				continue
			}

			r := execToolLocal(wsPath, tc.Function.Name, input)
			steps = append(steps, oaiAgentMessage{Type: "tool_result", Content: trunc(r, 3000)})
			messages = append(messages, map[string]interface{}{
				"role": "tool", "tool_call_id": tc.ID, "content": trunc(r, 6000),
			})
		}
		if delegated {
			break
		}
	}
	return oaiProcessResult{Messages: steps, NeedsSubAgent: subAgent}, nil
}

func execToolLocal(wsPath, name string, input map[string]interface{}) string {
	switch name {
	case "read_file":
		p, _ := input["path"].(string)
		if !filepath.IsAbs(p) { p = filepath.Join(wsPath, p) }
		d, err := os.ReadFile(p)
		if err != nil { return "Error: " + err.Error() }
		return string(d)
	case "write_file":
		p, _ := input["path"].(string)
		c, _ := input["content"].(string)
		if !filepath.IsAbs(p) { p = filepath.Join(wsPath, p) }
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(c), 0o644)
		return fmt.Sprintf("Written %s (%d bytes)", p, len(c))
	case "run_command":
		cmd, _ := input["command"].(string)
		c := exec.Command("bash", "-c", cmd)
		c.Dir = wsPath
		out, err := c.CombinedOutput()
		r := string(out)
		if err != nil { r += "\n[exit: " + err.Error() + "]" }
		return r
	case "list_files":
		p, _ := input["path"].(string)
		if !filepath.IsAbs(p) { p = filepath.Join(wsPath, p) }
		c := exec.Command("find", p, "-type", "f")
		out, _ := c.Output()
		return string(out)
	}
	return "unknown"
}

func trunc(s string, n int) string {
	if len(s) <= n { return s }
	return s[:n] + "..."
}

// --- The test ---

func (s *OpenAIDemoSuite) TestOpenAI_SubAgentScenario() {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Works with either Claude or OpenAI — uses callLLM which auto-selects.
	s.Require().True(os.Getenv("ANTHROPIC_API_KEY") != "" || os.Getenv("OPENAI_API_KEY") != "", "Need ANTHROPIC_API_KEY or OPENAI_API_KEY")

	taskQueue := testcore.RandomizeStr("llm-sub-tq")
	workflowID := testcore.RandomizeStr("llm-sub-wf")
	storeDir := filepath.Join(s.T().TempDir(), "ws-store")
	wsID := workflowID + "/ws"

	baseSystem := `You are an AI coding assistant with a persistent workspace.
Use tools to write files, read files, run commands.
The workspace has go.mod set up. Write Go code directly.
You can delegate to sub-agents using delegate_to_subagent tool.
Use "reviewer" (fork mode) for code review, "implementer" (handoff mode) for changes.
Be concise.`

	subAgentSystem := func(agentType string) string {
		switch agentType {
		case "reviewer":
			return "You are a code reviewer. Read the code, suggest improvements. Do NOT modify files. Be concise."
		case "implementer":
			return "You are a code implementer. Read the task, modify the code accordingly. Run 'go build ./...' to verify. Be concise."
		default:
			return "You are a specialized agent. Perform the requested task. Be concise."
		}
	}

	// --- Workflow with real sub-agent delegation ---
	agentWf := func(ctx workflow.Context) error {
		done := false
		workflow.SetUpdateHandler(ctx, "prompt", func(ctx workflow.Context, req oaiPromptReq) (oaiPromptResp, error) {
			if req.Prompt == "exit" {
				done = true
				return oaiPromptResp{Messages: []oaiAgentMessage{{Type: "text", Content: "Done"}}}, nil
			}

			ao := workflow.ActivityOptions{
				StartToCloseTimeout: 120 * time.Second,
				WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
			}
			actCtx := workflow.WithActivityOptions(ctx, ao)

			// Run main agent.
			var result oaiProcessResult
			workflow.ExecuteActivity(actCtx, "MainAgent", baseSystem, req.Prompt).Get(ctx, &result)

			allMsgs := result.Messages

			// Handle sub-agent delegation.
			if result.NeedsSubAgent != nil {
				sub := result.NeedsSubAgent
				allMsgs = append(allMsgs, oaiAgentMessage{
					Type:    "subagent",
					Content: fmt.Sprintf("→ Delegating to %s (%s mode)", sub.AgentType, sub.TransferMode),
				})

				tm := enumspb.WORKSPACE_TRANSFER_MODE_FORK
				if sub.TransferMode == "handoff" {
					tm = enumspb.WORKSPACE_TRANSFER_MODE_HANDOFF
				}

				childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					WorkflowExecutionTimeout: 120 * time.Second,
					WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
						{SourceWorkspaceID: wsID, Mode: tm},
					},
				})

				var childResult string
				err := workflow.ExecuteChildWorkflow(childCtx, "OAISubAgent", oaiSubAgentInput{
					WorkspaceID:  wsID,
					SystemPrompt: subAgentSystem(sub.AgentType),
					Task:         sub.Task,
				}).Get(ctx, &childResult)

				if err != nil {
					allMsgs = append(allMsgs, oaiAgentMessage{Type: "error", Content: err.Error()})
				} else {
					allMsgs = append(allMsgs, oaiAgentMessage{
						Type:    "subagent",
						Content: fmt.Sprintf("← %s returned: %s", sub.AgentType, trunc(childResult, 500)),
					})

					// Follow up: feed result back.
					followUp := fmt.Sprintf("The %s sub-agent finished. Its report:\n%s\nBriefly summarize.", sub.AgentType, childResult)
					var followResult oaiProcessResult
					workflow.ExecuteActivity(actCtx, "MainAgent", baseSystem, followUp).Get(ctx, &followResult)
					allMsgs = append(allMsgs, followResult.Messages...)
				}
			}
			return oaiPromptResp{Messages: allMsgs}, nil
		})
		return workflow.Await(ctx, func() bool { return done })
	}

	// Sub-agent child workflow.
	subAgentWf := func(ctx workflow.Context, input oaiSubAgentInput) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 120 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		actCtx := workflow.WithActivityOptions(ctx, ao)
		var result oaiProcessResult
		workflow.ExecuteActivity(actCtx, "MainAgent", input.SystemPrompt, input.Task).Get(ctx, &result)
		var out string
		for _, m := range result.Messages {
			if m.Type == "text" {
				out += m.Content + "\n"
			}
		}
		return out, nil
	}

	// Activity.
	mainAgent := func(ctx context.Context, system, prompt string) (oaiProcessResult, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return oaiProcessResult{}, err
		}
		modPath := filepath.Join(wsPath, "go.mod")
		if _, err := os.Stat(modPath); os.IsNotExist(err) {
			os.WriteFile(modPath, []byte("module example.com/demo\n\ngo 1.21\n"), 0o644)
		}
		return callLLM(ctx, system, prompt, wsPath)
	}

	// Worker.
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, converter.NewLocalFileStoreDriver(storeDir))
	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{WorkspaceManager: wsMgr})
	w.RegisterWorkflowWithOptions(agentWf, workflow.RegisterOptions{Name: "OAIMainAgent"})
	w.RegisterWorkflowWithOptions(subAgentWf, workflow.RegisterOptions{Name: "OAISubAgent"})
	w.RegisterActivityWithOptions(mainAgent, activity.RegisterOptions{Name: "MainAgent"})
	s.NoError(w.Start())
	defer w.Stop()

	// Start workflow.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: workflowID, TaskQueue: taskQueue,
	}, "OAIMainAgent")
	s.NoError(err)

	send := func(prompt string) oaiPromptResp {
		s.T().Logf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		s.T().Logf("💬 %s", prompt)
		s.T().Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		handle, err := s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			WorkflowID: workflowID, UpdateName: "prompt",
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
			Args:         []interface{}{oaiPromptReq{Prompt: prompt}},
		})
		s.NoError(err)
		var resp oaiPromptResp
		s.NoError(handle.Get(ctx, &resp))
		for _, m := range resp.Messages {
			switch m.Type {
			case "text":
				s.T().Logf("🤖 %s", m.Content)
			case "tool_call":
				s.T().Logf("⚡ %s: %s", m.ToolName, trunc(m.ToolInput, 200))
			case "tool_result":
				s.T().Logf("📄 %s", trunc(m.Content, 300))
			case "subagent":
				s.T().Logf("🔀 %s", m.Content)
			case "error":
				s.T().Logf("❌ %s", m.Content)
			}
		}
		return resp
	}

	// === SCENARIO ===
	s.T().Log("\n🚀 AI Coding Scenario with Sub-Agent Delegation (GPT-5.4-mini)\n")

	// Step 1: Write hello world.
	send("Write a simple hello world in Go. Just create main.go.")

	// Step 2: Build and run.
	send("Build and run it. Tell me what it prints.")

	// Step 3: Delegate review to sub-agent.
	send("Start a reviewer sub-agent to review the code and suggest adding a timestamp to the output.")

	// Step 4: Delegate implementation to sub-agent.
	send("Start an implementer sub-agent to implement the suggested timestamp change.")

	// Step 5: Build and run again.
	resp := send("Build and run the program. Show me the output.")
	foundTimestamp := false
	for _, m := range resp.Messages {
		if (m.Type == "tool_result" || m.Type == "text") && (strings.Contains(m.Content, "202") || strings.Contains(m.Content, "time")) {
			foundTimestamp = true
		}
	}
	s.True(foundTimestamp, "Expected output with timestamp after sub-agent modifications")

	s.T().Log("\n✅ Scenario complete with real sub-agent delegation!\n")

	send("exit")
	s.NoError(run.Get(ctx, nil))
}

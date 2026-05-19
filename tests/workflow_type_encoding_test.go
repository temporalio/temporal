package tests

import (
	"fmt"
	"strings"
	"testing"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// WorkflowTypeEncodingSuite verifies that workflow type names containing
// arbitrary bytes — including HTTP/2-unsafe control characters and multi-byte
// UTF-8 — can be used end-to-end without breaking gRPC transport.
//
// Workflow type names flow through several layers (mutable state, context
// metadata, gRPC response trailers via ContextMetadataInterceptor) and any
// layer that places raw strings into HTTP/2 headers will reject C0 control
// bytes (0x00-0x1F except HTAB 0x09) and DEL (0x7F). The server-side fix
// serializes all context metadata into a single protobuf message under the
// "contextmetadata-bin" gRPC trailer key. The "-bin" suffix causes gRPC to
// base64-encode the value on the wire (RFC 4648), making it safe for
// arbitrary byte sequences.
type WorkflowTypeEncodingSuite struct {
	parallelsuite.Suite[*WorkflowTypeEncodingSuite]
}

func TestWorkflowTypeEncodingSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowTypeEncodingSuite{})
}

func (s *WorkflowTypeEncodingSuite) runWithWorkflowType(env *testcore.TestEnv, workflowType string) error {
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: workflowType},
	)

	run, err := env.SdkClient().ExecuteWorkflow(
		env.Context(),
		sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf-trailer"),
			TaskQueue: env.WorkerTaskQueue(),
		},
		workflowType,
	)
	if err != nil {
		return err
	}
	return run.Get(env.Context(), nil)
}

func (s *WorkflowTypeEncodingSuite) TestPlainASCII() {
	s.Run("Succeeds", func(s *WorkflowTypeEncodingSuite) {
		env := testcore.NewEnv(s.T())
		s.NoError(s.runWithWorkflowType(env, "PlainAsciiWorkflowType"))
	})
}

func (s *WorkflowTypeEncodingSuite) TestControlCharsInWorkflowType() {
	cases := []struct {
		label string
		char  string
	}{
		{"HTAB (safe control char)", "\t"},
		{"newline", "\n"},
		{"carriage return", "\r"},
		{"CRLF", "\r\n"},
		{"NUL", "\x00"},
		{"bell", "\x07"},
		{"escape", "\x1b"},
		{"DEL", "\x7f"},
	}
	for _, tc := range cases {
		s.Run(fmt.Sprintf("control char %s succeeds", tc.label), func(s *WorkflowTypeEncodingSuite) {
			env := testcore.NewEnv(s.T())
			s.NoError(s.runWithWorkflowType(env, "Foo"+tc.char+"Bar"))
		})
	}
}

func (s *WorkflowTypeEncodingSuite) TestAllControlCharsWorkflowType() {
	s.Run("only control chars succeeds", func(s *WorkflowTypeEncodingSuite) {
		env := testcore.NewEnv(s.T())
		s.NoError(s.runWithWorkflowType(env, "\n\x00\r"))
	})
}

func (s *WorkflowTypeEncodingSuite) TestLongWorkflowType() {
	s.Run("succeeds", func(s *WorkflowTypeEncodingSuite) {
		env := testcore.NewEnv(s.T())
		longName := strings.Repeat("a", 999)
		s.NoError(s.runWithWorkflowType(env, longName))
	})
}

func (s *WorkflowTypeEncodingSuite) TestWorkflowTypeEndingInBin() {
	s.Run("succeeds", func(s *WorkflowTypeEncodingSuite) {
		env := testcore.NewEnv(s.T())
		s.NoError(s.runWithWorkflowType(env, "my-workflow-bin"))
	})
}

func (s *WorkflowTypeEncodingSuite) TestUTF8WorkflowType() {
	cases := []struct {
		label        string
		workflowType string
	}{
		{"CJK", "Workflow-日本語"},
		{"emoji", "🚀-workflow"},
		{"accented", "cafe-resume"},
		{"mixed", "uber-naive-🎉-工作流"},
	}
	for _, tc := range cases {
		s.Run(fmt.Sprintf("UTF-8 %s succeeds", tc.label), func(s *WorkflowTypeEncodingSuite) {
			env := testcore.NewEnv(s.T())
			s.NoError(s.runWithWorkflowType(env, tc.workflowType))
		})
	}
}

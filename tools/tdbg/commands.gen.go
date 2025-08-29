// Code generated. DO NOT EDIT.

package tdbg

import (
	"github.com/mattn/go-isatty"

	"github.com/spf13/cobra"

	"os"
)

var hasHighlighting = isatty.IsTerminal(os.Stdout.Fd())

type TdbgCommand struct {
	Command                    cobra.Command
	Address                    string
	Namespace                  string
	ContextTimeout             int
	Yes                        bool
	TlsCertPath                string
	TlsKeyPath                 string
	TlsCaPath                  string
	TlsDisableHostVerification bool
	TlsServerName              string
	Color                      StringEnum
}

func NewTdbgCommand(cctx *CommandContext) *TdbgCommand {
	var s TdbgCommand
	s.Command.Use = "tdbg"
	s.Command.Short = "Run admin operation on Temporal server"
	s.Command.Long = "A command-line tool for Temporal server debugging"
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgDecodeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgHistoryHostCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgMembershipCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgShardCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgTaskqueueCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowCommand(cctx, &s).Command)
	s.Command.PersistentFlags().StringVar(&s.Address, "address", "", "Host:port for Temporal frontend service.")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("address"), "TEMPORAL_CLI_ADDRESS")
	s.Command.PersistentFlags().StringVar(&s.Namespace, "namespace", "default", "Temporal Workflow namespace. Aliased as \"--n\".")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("namespace"), "TEMPORAL_CLI_NAMESPACE")
	s.Command.PersistentFlags().IntVar(&s.ContextTimeout, "context-timeout", 5, "Timeout in seconds for RPC calls. Aliased as \"--ct\".")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("context-timeout"), "TEMPORAL_CONTEXT_TIMEOUT")
	s.Command.PersistentFlags().BoolVar(&s.Yes, "yes", false, "Automatically confirm all prompts.")
	s.Command.PersistentFlags().StringVar(&s.TlsCertPath, "tls-cert-path", "", "Path to x509 certificate.")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("tls-cert-path"), "TEMPORAL_CLI_TLS_CERT")
	s.Command.PersistentFlags().StringVar(&s.TlsKeyPath, "tls-key-path", "", "Path to private key.")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("tls-key-path"), "TEMPORAL_CLI_TLS_KEY")
	s.Command.PersistentFlags().StringVar(&s.TlsCaPath, "tls-ca-path", "", "Path to server CA certificate.")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("tls-ca-path"), "TEMPORAL_CLI_TLS_CA")
	s.Command.PersistentFlags().BoolVar(&s.TlsDisableHostVerification, "tls-disable-host-verification", false, "Disable TLS hostname verification (TLS must be enabled).")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("tls-disable-host-verification"), "TEMPORAL_CLI_TLS_DISABLE_HOST_VERIFICATION")
	s.Command.PersistentFlags().StringVar(&s.TlsServerName, "tls-server-name", "", "Override for target server name.")
	cctx.BindFlagEnvVar(s.Command.PersistentFlags().Lookup("tls-server-name"), "TEMPORAL_CLI_TLS_SERVER_NAME")
	s.Color = NewStringEnum([]string{"auto", "always", "never"}, "auto")
	s.Command.PersistentFlags().Var(&s.Color, "color", "When to use color auto, always, never. Accepted values: auto, always, never.")
	s.Command.PersistentFlags().SetNormalizeFunc(aliasNormalizer(map[string]string{
		"ct": "context-timeout",
		"n":  "namespace",
	}))
	s.initCommand(cctx)
	return &s
}

type TdbgDecodeCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgDecodeCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgDecodeCommand {
	var s TdbgDecodeCommand
	s.Parent = parent
	s.Command.Use = "decode"
	s.Command.Short = "Decode payload"
	s.Command.Long = "This command will decode payloads."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgDecodeBase64Command(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDecodeProtoCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDecodeTaskCommand(cctx, &s).Command)
	return &s
}

type TdbgDecodeBase64Command struct {
	Parent     *TdbgDecodeCommand
	Command    cobra.Command
	Base64Data string
	Base64File string
}

func NewTdbgDecodeBase64Command(cctx *CommandContext, parent *TdbgDecodeCommand) *TdbgDecodeBase64Command {
	var s TdbgDecodeBase64Command
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "base64 [flags]"
	s.Command.Short = "Decode base64 payload"
	s.Command.Long = "Decode base64-encoded data into raw or human-readable output."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.Base64Data, "base64-data", "", "Data in base64 format (e.g. anNvbi9wbGFpbg==).")
	s.Command.Flags().StringVar(&s.Base64File, "base64-file", "", "Path to a file with base64 encoded data.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDecodeProtoCommand struct {
	Parent     *TdbgDecodeCommand
	Command    cobra.Command
	ProtoType  string
	HexData    string
	HexFile    string
	BinaryFile string
}

func NewTdbgDecodeProtoCommand(cctx *CommandContext, parent *TdbgDecodeCommand) *TdbgDecodeProtoCommand {
	var s TdbgDecodeProtoCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "proto [flags]"
	s.Command.Short = "Decode proto payload"
	s.Command.Long = "Decode a hex or binary encoded proto payload into a structured message."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.ProtoType, "proto-type", "", "Full name of the proto type to decode to (e.g. temporal.server.api.persistence.v1.WorkflowExecutionInfo).")
	s.Command.Flags().StringVar(&s.HexData, "hex-data", "", "Data in hex format (e.g. 0x0a243462613036633466...).")
	s.Command.Flags().StringVar(&s.HexFile, "hex-file", "", "Path to a file with data in hex format.")
	s.Command.Flags().StringVar(&s.BinaryFile, "binary-file", "", "Path to a file with data in binary format.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDecodeTaskCommand struct {
	Parent         *TdbgDecodeCommand
	Command        cobra.Command
	BinaryFile     string
	TaskCategoryId int
	Encoding       string
}

func NewTdbgDecodeTaskCommand(cctx *CommandContext, parent *TdbgDecodeCommand) *TdbgDecodeTaskCommand {
	var s TdbgDecodeTaskCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "task [flags]"
	s.Command.Short = "Decode a history task blob"
	s.Command.Long = "Decode a history task blob in binary format into a JSON message."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.BinaryFile, "binary-file", "", "Path to a binary file containing the task blob. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "binary-file")
	s.Command.Flags().IntVar(&s.TaskCategoryId, "task-category-id", 0, "Task category ID (see the history/tasks package). Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "task-category-id")
	s.Command.Flags().StringVar(&s.Encoding, "encoding", "", "Encoding type (see temporal.api.enums.v1.EncodingType). Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "encoding")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqCommand struct {
	Parent     *TdbgCommand
	Command    cobra.Command
	DlqVersion StringEnum
}

func NewTdbgDlqCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgDlqCommand {
	var s TdbgDlqCommand
	s.Parent = parent
	s.Command.Use = "dlq"
	s.Command.Short = "Run admin operations on the DLQ"
	s.Command.Long = "This command will run admin operations on the DLQ."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgDlqJobCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqListCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqMergeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqPurgeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqReadCommand(cctx, &s).Command)
	s.DlqVersion = NewStringEnum([]string{"v1", "v2"}, "v2")
	s.Command.PersistentFlags().Var(&s.DlqVersion, "dlq-version", "Version of DLQ to manage. Options \"v1\", \"v2\". Accepted values: v1, v2.")
	return &s
}

type TdbgDlqJobCommand struct {
	Parent  *TdbgDlqCommand
	Command cobra.Command
}

func NewTdbgDlqJobCommand(cctx *CommandContext, parent *TdbgDlqCommand) *TdbgDlqJobCommand {
	var s TdbgDlqJobCommand
	s.Parent = parent
	s.Command.Use = "job"
	s.Command.Short = "Run admin operation on DLQ Job"
	s.Command.Long = "Execute operations on a DLQ Job. See subcommands under this command for job-level controls."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgDlqJobCancelCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgDlqJobDescribeCommand(cctx, &s).Command)
	return &s
}

type TdbgDlqJobCancelCommand struct {
	Parent   *TdbgDlqJobCommand
	Command  cobra.Command
	JobToken string
	Reason   string
}

func NewTdbgDlqJobCancelCommand(cctx *CommandContext, parent *TdbgDlqJobCommand) *TdbgDlqJobCancelCommand {
	var s TdbgDlqJobCancelCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "cancel [flags]"
	s.Command.Short = "Cancel the DLQ job with provided job token"
	if hasHighlighting {
		s.Command.Long = "This command will cancel the DLQ job with the provided job token.\nA reason for cancellation must also be provided to explain why the job is being terminated. Both values are required.\nThe job token can be obtained from the output of a previous \x1b[1mmerge\x1b[0m or \x1b[1mpurge\x1b[0m command.\nExample invocation: \x1b[1mtdbg dlq job cancel \\\n    --job-token YourJobToken \\\n    --reason \"Manual cancellation due to task failure\"\x1b[0m"
	} else {
		s.Command.Long = "This command will cancel the DLQ job with the provided job token.\nA reason for cancellation must also be provided to explain why the job is being terminated. Both values are required.\nThe job token can be obtained from the output of a previous `merge` or `purge` command.\nExample invocation: ``` tdbg dlq job cancel \\\n    --job-token YourJobToken \\\n    --reason \"Manual cancellation due to task failure\"\n```"
	}
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.JobToken, "job-token", "", "Token of the DLQ job. This token will be printed in the output of merge and purge commands. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "job-token")
	s.Command.Flags().StringVar(&s.Reason, "reason", "", "Reason for job cancellation. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "reason")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqJobDescribeCommand struct {
	Parent   *TdbgDlqJobCommand
	Command  cobra.Command
	JobToken string
}

func NewTdbgDlqJobDescribeCommand(cctx *CommandContext, parent *TdbgDlqJobCommand) *TdbgDlqJobDescribeCommand {
	var s TdbgDlqJobDescribeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "describe [flags]"
	s.Command.Short = "Get details of the DLQ job with provided job token"
	if hasHighlighting {
		s.Command.Long = "This command will get details of the DLQ job with the provided job token if using v2.\nThe token can be obtained from the output of the \x1b[1mmerge\x1b[0m or \x1b[1mpurge\x1b[0m DLQ commands.\nExample invocation: \x1b[1mtdbg dlq job describe \\\n    --job-token YourJobToken\x1b[0m"
	} else {
		s.Command.Long = "This command will get details of the DLQ job with the provided job token if using v2.\nThe token can be obtained from the output of the `merge` or `purge` DLQ commands.\nExample invocation: ``` tdbg dlq job describe \\\n    --job-token YourJobToken\n```"
	}
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.JobToken, "job-token", "", "Token of the DLQ job. This token will be printed in the output of merge and purge commands. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "job-token")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqListCommand struct {
	Parent     *TdbgDlqCommand
	Command    cobra.Command
	OutputFile string
	PageSize   int
	PrintJson  bool
}

func NewTdbgDlqListCommand(cctx *CommandContext, parent *TdbgDlqCommand) *TdbgDlqListCommand {
	var s TdbgDlqListCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "list [flags]"
	s.Command.Short = "List all DLQs,only supported for v2"
	s.Command.Long = "List all DLQs (only supported for v2). Outputs JSON and supports pagination."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.OutputFile, "output-file", "", "Output file path. Defaults to stdout.")
	s.Command.Flags().IntVar(&s.PageSize, "page-size", 10, "Page size when listing queues from the DB.")
	s.Command.Flags().BoolVar(&s.PrintJson, "print-json", false, "Print raw JSON output.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqMergeCommand struct {
	Parent        *TdbgDlqCommand
	Command       cobra.Command
	SourceCluster string
	TargetCluster string
	ShardId       int
	PageSize      int
	LastMessageId int
	DlqType       StringEnum
}

func NewTdbgDlqMergeCommand(cctx *CommandContext, parent *TdbgDlqCommand) *TdbgDlqMergeCommand {
	var s TdbgDlqMergeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "merge [flags]"
	s.Command.Short = "Merge DLQ messages"
	s.Command.Long = "Merge (i.e., re-enqueue then delete) DLQ messages with equal or smaller task IDs. Requires v2 DLQ."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.SourceCluster, "source-cluster", "", "Source cluster for the DLQ.")
	s.Command.Flags().StringVar(&s.TargetCluster, "target-cluster", "", "Target cluster for the DLQ.")
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "Shard ID of the DLQ message.")
	s.Command.Flags().IntVar(&s.PageSize, "page-size", 0, "Batch size used during message purging.")
	s.Command.Flags().IntVar(&s.LastMessageId, "last-message-id", 0, "The upper boundary of messages to operate on. If not provided, all messages will be operated on. However, you will be prompted for confirmation unless the --yes flag is also provided.")
	s.DlqType = NewStringEnum([]string{"namespace", "history", "transfer", "timer", "replication", "visibility"}, "")
	s.Command.Flags().Var(&s.DlqType, "dlq-type", "Type of DLQ to merge. Accepted values: namespace, history, transfer, timer, replication, visibility. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "dlq-type")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqPurgeCommand struct {
	Parent        *TdbgDlqCommand
	Command       cobra.Command
	SourceCluster string
	TargetCluster string
	ShardId       int
	LastMessageId int
	DlqType       StringEnum
}

func NewTdbgDlqPurgeCommand(cctx *CommandContext, parent *TdbgDlqCommand) *TdbgDlqPurgeCommand {
	var s TdbgDlqPurgeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "purge [flags]"
	s.Command.Short = "Delete DLQ messages"
	s.Command.Long = "Delete DLQ messages with equal or smaller IDs than the specified task ID."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.SourceCluster, "source-cluster", "", "Source cluster for the DLQ.")
	s.Command.Flags().StringVar(&s.TargetCluster, "target-cluster", "", "Target cluster for the DLQ.")
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "Shard ID of the DLQ message.")
	s.Command.Flags().IntVar(&s.LastMessageId, "last-message-id", 0, "The upper boundary of messages to operate on. If not provided, all messages will be operated on. However, you will be prompted for confirmation unless the --yes flag is also provided.")
	s.DlqType = NewStringEnum([]string{"namespace", "history", "transfer", "timer", "replication", "visibility"}, "")
	s.Command.Flags().Var(&s.DlqType, "dlq-type", "Type of DLQ to purge. Accepted values: namespace, history, transfer, timer, replication, visibility. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "dlq-type")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgDlqReadCommand struct {
	Parent          *TdbgDlqCommand
	Command         cobra.Command
	SourceCluster   string
	TargetCluster   string
	ShardId         int
	MaxMessageCount int
	OutputFile      string
	PageSize        int
	LastMessageId   int
	DlqType         StringEnum
}

func NewTdbgDlqReadCommand(cctx *CommandContext, parent *TdbgDlqCommand) *TdbgDlqReadCommand {
	var s TdbgDlqReadCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "read [flags]"
	s.Command.Short = "Read DLQ Messages"
	s.Command.Long = "Read messages from the Dead Letter Queue (DLQ). Supports pagination and writing output to a file or stdout."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.SourceCluster, "source-cluster", "", "Source cluster for the DLQ.")
	s.Command.Flags().StringVar(&s.TargetCluster, "target-cluster", "", "Target cluster for the DLQ.")
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "Shard ID of the DLQ message.")
	s.Command.Flags().IntVar(&s.MaxMessageCount, "max-message-count", 100, "Max number of messages to fetch. Only applicable to v2.")
	s.Command.Flags().StringVar(&s.OutputFile, "output-file", "", "File to write output to (defaults to stdout if not specified).")
	s.Command.Flags().IntVar(&s.PageSize, "page-size", 10, "Page size to use when reading messages from DB (v2 only).")
	s.Command.Flags().IntVar(&s.LastMessageId, "last-message-id", 0, "The upper boundary of messages to operate on. If not provided, all messages will be operated on. However, you will be prompted for confirmation unless the --yes flag is also provided.")
	s.DlqType = NewStringEnum([]string{"namespace", "history", "transfer", "timer", "replication", "visibility"}, "")
	s.Command.Flags().Var(&s.DlqType, "dlq-type", "Type of DLQ to manage. Accepted values: namespace, history, transfer, timer, replication, visibility. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "dlq-type")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgHistoryHostCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgHistoryHostCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgHistoryHostCommand {
	var s TdbgHistoryHostCommand
	s.Parent = parent
	s.Command.Use = "history-host"
	s.Command.Short = "Run admin operations on a history host"
	s.Command.Long = "This command will run admin operations on a history host."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgHistoryHostDescribeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgHistoryHostGetShardIdCommand(cctx, &s).Command)
	return &s
}

type TdbgHistoryHostDescribeCommand struct {
	Parent           *TdbgHistoryHostCommand
	Command          cobra.Command
	WorkflowId       string
	HistoryAddress   string
	ShardId          int
	PrintFullyDetail bool
}

func NewTdbgHistoryHostDescribeCommand(cctx *CommandContext, parent *TdbgHistoryHostCommand) *TdbgHistoryHostDescribeCommand {
	var s TdbgHistoryHostDescribeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "describe [flags]"
	s.Command.Short = "Describe internal information of history host"
	s.Command.Long = "This command will describe the internal information of a history host."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "The ID of the workflow.")
	s.Command.Flags().StringVar(&s.HistoryAddress, "history-address", "", "History Host address (IP:PORT).")
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "The ID of the shard.")
	s.Command.Flags().BoolVar(&s.PrintFullyDetail, "print-fully-detail", false, "Print fully detailed information.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgHistoryHostGetShardIdCommand struct {
	Parent         *TdbgHistoryHostCommand
	Command        cobra.Command
	NamespaceId    string
	WorkflowId     string
	NumberOfShards int
}

func NewTdbgHistoryHostGetShardIdCommand(cctx *CommandContext, parent *TdbgHistoryHostCommand) *TdbgHistoryHostGetShardIdCommand {
	var s TdbgHistoryHostGetShardIdCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "get-shard-id [flags]"
	s.Command.Short = "Get shard ID for a namespace ID and workflow ID combination"
	s.Command.Long = "This command will retrieve the shard ID for a given namespace ID and workflow ID combination."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.NamespaceId, "namespace-id", "", "The ID of the namespace.")
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "The ID of the workflow.")
	s.Command.Flags().IntVar(&s.NumberOfShards, "number-of-shards", 0, "Number of shards for the Temporal cluster (check config for `numHistoryShards`).")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgMembershipCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgMembershipCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgMembershipCommand {
	var s TdbgMembershipCommand
	s.Parent = parent
	s.Command.Use = "membership"
	s.Command.Short = "Run admin operations on cluster membership"
	s.Command.Long = "This command will run admin operations on cluster membership."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgMembershipListDbCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgMembershipListGossipCommand(cctx, &s).Command)
	return &s
}

type TdbgMembershipListDbCommand struct {
	Parent            *TdbgMembershipCommand
	Command           cobra.Command
	HeartbeatedWithin string
	Role              string
}

func NewTdbgMembershipListDbCommand(cctx *CommandContext, parent *TdbgMembershipCommand) *TdbgMembershipListDbCommand {
	var s TdbgMembershipListDbCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "list-db [flags]"
	s.Command.Short = "List cluster membership items"
	s.Command.Long = "This command lists cluster membership records from the database for Temporal nodes."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.HeartbeatedWithin, "heartbeated-within", "15m", "Filter by last heartbeat time. Supported formats:   - '2006-01-02T15:04:05+07:00'   - raw UnixNano   - time range (e.g., '15m', '1h', '7d'). Default is '15m' (last 15 minutes).")
	s.Command.Flags().StringVar(&s.Role, "role", "all", "Membership role filter (frontend, history, matching, worker, or all).")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgMembershipListGossipCommand struct {
	Parent  *TdbgMembershipCommand
	Command cobra.Command
	Role    string
}

func NewTdbgMembershipListGossipCommand(cctx *CommandContext, parent *TdbgMembershipCommand) *TdbgMembershipListGossipCommand {
	var s TdbgMembershipListGossipCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "list-gossip [flags]"
	s.Command.Short = "List ringpop membership items"
	s.Command.Long = "This command lists Ringpop gossip-based membership items for Temporal cluster nodes."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.Role, "role", "all", "Membership role filter (frontend, history, matching, worker, or all).")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgShardCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgShardCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgShardCommand {
	var s TdbgShardCommand
	s.Parent = parent
	s.Command.Use = "shard"
	s.Command.Short = "Run admin operations on a specific shard"
	s.Command.Long = "This command will run admin operations on a specific shard."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgShardCloseShardCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgShardDescribeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgShardListTasksCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgShardRemoveTaskCommand(cctx, &s).Command)
	return &s
}

type TdbgShardCloseShardCommand struct {
	Parent  *TdbgShardCommand
	Command cobra.Command
	ShardId int
}

func NewTdbgShardCloseShardCommand(cctx *CommandContext, parent *TdbgShardCommand) *TdbgShardCloseShardCommand {
	var s TdbgShardCloseShardCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "close-shard [flags]"
	s.Command.Short = "Close a shard by ID"
	s.Command.Long = "This command will close a shard given a shard ID."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "Shard ID for the Temporal cluster to manage. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "shard-id")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgShardDescribeCommand struct {
	Parent  *TdbgShardCommand
	Command cobra.Command
	ShardId int
}

func NewTdbgShardDescribeCommand(cctx *CommandContext, parent *TdbgShardCommand) *TdbgShardDescribeCommand {
	var s TdbgShardDescribeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "describe [flags]"
	s.Command.Short = "Describe shard by ID"
	s.Command.Long = "This command will describe a shard by its ID."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "The ID of the shard to describe. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "shard-id")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgShardListTasksCommand struct {
	Parent                 *TdbgShardCommand
	Command                cobra.Command
	More                   bool
	PageSize               int
	ShardId                int
	TaskCategory           string
	MinTaskId              int
	MaxTaskId              int
	MinVisibilityTimestamp string
	MaxVisibilityTimestamp string
	PrintJson              bool
}

func NewTdbgShardListTasksCommand(cctx *CommandContext, parent *TdbgShardCommand) *TdbgShardListTasksCommand {
	var s TdbgShardListTasksCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "list-tasks [flags]"
	s.Command.Short = "List tasks for a shard and task category"
	s.Command.Long = "This command will list tasks for the given shard ID and task category."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().BoolVar(&s.More, "more", false, "List more pages. Default is to list one page of default size 10.")
	s.Command.Flags().IntVar(&s.PageSize, "page-size", 1000, "Result page size.")
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "The ID of the shard. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "shard-id")
	s.Command.Flags().StringVar(&s.TaskCategory, "task-category", "", "The task category to list (e.g. transfer, timer, replication, visibility). Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "task-category")
	s.Command.Flags().IntVar(&s.MinTaskId, "min-task-id", 0, "Inclusive min task ID. Optional for transfer, replication, visibility. Not allowed for timer tasks.")
	s.Command.Flags().IntVar(&s.MaxTaskId, "max-task-id", 0, "Exclusive max task ID. Required for transfer, replication, visibility. Not allowed for timer tasks.")
	s.Command.Flags().StringVar(&s.MinVisibilityTimestamp, "min-visibility-timestamp", "", "Inclusive min task fire timestamp. Optional for timer tasks. Not allowed for transfer, replication, visibility tasks. Supported formats: '2006-01-02T15:04:05+07:00', raw UnixNano, or time range (e.g. '15m', '1h').")
	s.Command.Flags().StringVar(&s.MaxVisibilityTimestamp, "max-visibility-timestamp", "", "Exclusive max task fire timestamp. Required for timer tasks. Not allowed for transfer, replication, visibility tasks. Supported formats: '2006-01-02T15:04:05+07:00', raw UnixNano, or time range (e.g. '15m', '1h').")
	s.Command.Flags().BoolVar(&s.PrintJson, "print-json", false, "Print in raw JSON format.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgShardRemoveTaskCommand struct {
	Parent                  *TdbgShardCommand
	Command                 cobra.Command
	ShardId                 int
	TaskId                  int
	TaskCategory            string
	TaskVisibilityTimestamp int
}

func NewTdbgShardRemoveTaskCommand(cctx *CommandContext, parent *TdbgShardCommand) *TdbgShardRemoveTaskCommand {
	var s TdbgShardRemoveTaskCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "remove-task [flags]"
	s.Command.Short = "Remove a task by shard ID, task category, task ID, and visibility timestamp"
	s.Command.Long = "This command will remove a task using the shard ID, task category, task ID, and visibility timestamp."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().IntVar(&s.ShardId, "shard-id", 0, "The ID of the shard. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "shard-id")
	s.Command.Flags().IntVar(&s.TaskId, "task-id", 0, "The ID of the task. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "task-id")
	s.Command.Flags().StringVar(&s.TaskCategory, "task-category", "", "The task category (e.g. transfer, timer, replication, visibility).")
	s.Command.Flags().IntVar(&s.TaskVisibilityTimestamp, "task-visibility-timestamp", 0, "Task visibility timestamp in nanoseconds (required for removing a timer task).")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgTaskqueueCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgTaskqueueCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgTaskqueueCommand {
	var s TdbgTaskqueueCommand
	s.Parent = parent
	s.Command.Use = "taskqueue"
	s.Command.Short = "Run admin operations on a task queue"
	s.Command.Long = "This command will run admin operations on a task queue."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgTaskqueueDescribeTaskQueuePartitionCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgTaskqueueForceUnloadTaskQueuePartitionCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgTaskqueueListTasksCommand(cctx, &s).Command)
	return &s
}

type TdbgTaskqueueDescribeTaskQueuePartitionCommand struct {
	Parent        *TdbgTaskqueueCommand
	Command       cobra.Command
	NamespaceId   string
	TaskQueue     string
	TaskQueueType string
	PartitionId   int
	StickyName    string
	BuildIds      []string
	Unversioned   bool
	AllActive     bool
}

func NewTdbgTaskqueueDescribeTaskQueuePartitionCommand(cctx *CommandContext, parent *TdbgTaskqueueCommand) *TdbgTaskqueueDescribeTaskQueuePartitionCommand {
	var s TdbgTaskqueueDescribeTaskQueuePartitionCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "describe-task-queue-partition [flags]"
	s.Command.Short = "Describe information related to a task queue partition"
	s.Command.Long = "This command will describe the partition information for a given task queue."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.NamespaceId, "namespace-id", "default", "Namespace ID.")
	s.Command.Flags().StringVar(&s.TaskQueue, "task-queue", "", "Task Queue name. Required.")
	s.Command.Flags().StringVar(&s.TaskQueueType, "task-queue-type", "TASK_QUEUE_TYPE_WORKFLOW", "Task Queue type (activity, workflow, nexus).")
	s.Command.Flags().IntVar(&s.PartitionId, "partition-id", 0, "Partition ID. Default is 0.")
	s.Command.Flags().StringVar(&s.StickyName, "sticky-name", "", "Sticky Name for a task queue partition.")
	s.Command.Flags().StringArrayVar(&s.BuildIds, "build-ids", nil, "List of Build IDs.")
	s.Command.Flags().BoolVar(&s.Unversioned, "unversioned", false, "Whether the task queue partition is unversioned. Default is true.")
	s.Command.Flags().BoolVar(&s.AllActive, "all-active", false, "Whether to include all active task queue versions. Default is true.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgTaskqueueForceUnloadTaskQueuePartitionCommand struct {
	Parent        *TdbgTaskqueueCommand
	Command       cobra.Command
	NamespaceId   string
	TaskQueue     string
	TaskQueueType string
	PartitionId   int
	StickyName    string
}

func NewTdbgTaskqueueForceUnloadTaskQueuePartitionCommand(cctx *CommandContext, parent *TdbgTaskqueueCommand) *TdbgTaskqueueForceUnloadTaskQueuePartitionCommand {
	var s TdbgTaskqueueForceUnloadTaskQueuePartitionCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "force-unload-task-queue-partition [flags]"
	s.Command.Short = "Forcefully unload a task queue partition"
	s.Command.Long = "This command will forcefully unload a task queue partition."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.NamespaceId, "namespace-id", "default", "Namespace ID.")
	s.Command.Flags().StringVar(&s.TaskQueue, "task-queue", "", "Task Queue name. Required.")
	s.Command.Flags().StringVar(&s.TaskQueueType, "task-queue-type", "TASK_QUEUE_TYPE_WORKFLOW", "Task Queue type: activity, workflow, nexus(experimental).")
	s.Command.Flags().IntVar(&s.PartitionId, "partition-id", 0, "Partition ID.")
	s.Command.Flags().StringVar(&s.StickyName, "sticky-name", "", "Sticky Name for a task queue partition.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgTaskqueueListTasksCommand struct {
	Parent        *TdbgTaskqueueCommand
	Command       cobra.Command
	More          bool
	PageSize      int
	TaskQueueType string
	TaskQueue     string
	MinTaskId     int
	MaxTaskId     int
	Subqueue      int
	PrintJson     bool
	Fair          bool
	MinPass       int
}

func NewTdbgTaskqueueListTasksCommand(cctx *CommandContext, parent *TdbgTaskqueueCommand) *TdbgTaskqueueListTasksCommand {
	var s TdbgTaskqueueListTasksCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "list-tasks [flags]"
	s.Command.Short = "List tasks of a task queue. Use --fair to list fairness tasks"
	if hasHighlighting {
		s.Command.Long = "This command will list tasks of a task queue. Use the \x1b[1m--fair\x1b[0m flag to list fairness tasks."
	} else {
		s.Command.Long = "This command will list tasks of a task queue. Use the `--fair` flag to list fairness tasks."
	}
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().BoolVar(&s.More, "more", false, "List more pages, default is to list one page of default page size 10.")
	s.Command.Flags().IntVar(&s.PageSize, "page-size", 0, "Result page size. Default is 10.")
	s.Command.Flags().StringVar(&s.TaskQueueType, "task-queue-type", "activity", "Task Queue type (activity, workflow).")
	s.Command.Flags().StringVar(&s.TaskQueue, "task-queue", "", "Task Queue name.")
	s.Command.Flags().IntVar(&s.MinTaskId, "min-task-id", 0, "Minimum task ID. Default is -12346.")
	s.Command.Flags().IntVar(&s.MaxTaskId, "max-task-id", 0, "Maximum task ID.")
	s.Command.Flags().IntVar(&s.Subqueue, "subqueue", 0, "Subqueue to query. Default is 0.")
	s.Command.Flags().BoolVar(&s.PrintJson, "print-json", false, "Print in raw JSON format.")
	s.Command.Flags().BoolVar(&s.Fair, "fair", false, "Query fairness tasks.")
	s.Command.Flags().IntVar(&s.MinPass, "min-pass", 0, "Minimum pass for fairness tasks. Default is 1.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowCommand struct {
	Parent  *TdbgCommand
	Command cobra.Command
}

func NewTdbgWorkflowCommand(cctx *CommandContext, parent *TdbgCommand) *TdbgWorkflowCommand {
	var s TdbgWorkflowCommand
	s.Parent = parent
	s.Command.Use = "workflow"
	s.Command.Short = "Run admin operations on a workflow"
	s.Command.Long = "This command will run admin operations on a workflow."
	s.Command.Args = cobra.NoArgs
	s.Command.AddCommand(&NewTdbgWorkflowDeleteCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowDescribeCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowImportCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowRebuildCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowRefreshTasksCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowReplicateCommand(cctx, &s).Command)
	s.Command.AddCommand(&NewTdbgWorkflowShowCommand(cctx, &s).Command)
	return &s
}

type TdbgWorkflowDeleteCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
}

func NewTdbgWorkflowDeleteCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowDeleteCommand {
	var s TdbgWorkflowDeleteCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "delete [flags]"
	s.Command.Short = "Delete the current workflow execution and mutable state record"
	s.Command.Long = "This command will delete the current workflow execution and the mutable state record."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to delete. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to delete.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowDescribeCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
}

func NewTdbgWorkflowDescribeCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowDescribeCommand {
	var s TdbgWorkflowDescribeCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "describe [flags]"
	s.Command.Short = "Describe internal information of workflow execution"
	s.Command.Long = "This command will describe internal information of workflow execution."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to describe. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to describe.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowImportCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
	InputFile  string
}

func NewTdbgWorkflowImportCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowImportCommand {
	var s TdbgWorkflowImportCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "import [flags]"
	s.Command.Short = "Import workflow history to database"
	s.Command.Long = "This command will import workflow history to the database."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to import history for. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to import history for.")
	s.Command.Flags().StringVar(&s.InputFile, "input-file", "", "Input file to import history from. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "input-file")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowRebuildCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
}

func NewTdbgWorkflowRebuildCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowRebuildCommand {
	var s TdbgWorkflowRebuildCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "rebuild [flags]"
	s.Command.Short = "Rebuild a workflow mutable state using persisted history events"
	s.Command.Long = "This command will rebuild a workflow mutable state using persisted history events."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to rebuild. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to rebuild.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowRefreshTasksCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
}

func NewTdbgWorkflowRefreshTasksCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowRefreshTasksCommand {
	var s TdbgWorkflowRefreshTasksCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "refresh-tasks [flags]"
	s.Command.Short = "Refresh all tasks of a workflow"
	s.Command.Long = "This command will refresh all tasks of a workflow."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to refresh tasks for. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to refresh tasks for.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowReplicateCommand struct {
	Parent     *TdbgWorkflowCommand
	Command    cobra.Command
	WorkflowId string
	RunId      string
}

func NewTdbgWorkflowReplicateCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowReplicateCommand {
	var s TdbgWorkflowReplicateCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "replicate [flags]"
	s.Command.Short = "Force replicate a workflow by generating replication tasks"
	s.Command.Long = "This command will force replicate a workflow by generating replication tasks."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to replicate. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to replicate.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

type TdbgWorkflowShowCommand struct {
	Parent          *TdbgWorkflowCommand
	Command         cobra.Command
	WorkflowId      string
	RunId           string
	MinEventId      int
	MaxEventId      int
	MinEventVersion int
	MaxEventVersion int
	OutputFile      string
}

func NewTdbgWorkflowShowCommand(cctx *CommandContext, parent *TdbgWorkflowCommand) *TdbgWorkflowShowCommand {
	var s TdbgWorkflowShowCommand
	s.Parent = parent
	s.Command.DisableFlagsInUseLine = true
	s.Command.Use = "show [flags]"
	s.Command.Short = "Show workflow history from the database"
	s.Command.Long = "This command will show workflow history from the database."
	s.Command.Args = cobra.NoArgs
	s.Command.Flags().StringVar(&s.WorkflowId, "workflow-id", "", "Workflow ID to retrieve history for. Required.")
	_ = cobra.MarkFlagRequired(s.Command.Flags(), "workflow-id")
	s.Command.Flags().StringVar(&s.RunId, "run-id", "", "Run ID to retrieve history for.")
	s.Command.Flags().IntVar(&s.MinEventId, "min-event-id", 0, "Minimum event ID to be included in the history.")
	s.Command.Flags().IntVar(&s.MaxEventId, "max-event-id", 9223372036854775807, "Maximum event ID to be included in the history.")
	s.Command.Flags().IntVar(&s.MinEventVersion, "min-event-version", 0, "Start event version to be included in the history.")
	s.Command.Flags().IntVar(&s.MaxEventVersion, "max-event-version", 0, "End event version to be included in the history.")
	s.Command.Flags().StringVar(&s.OutputFile, "output-file", "", "Output file to store the workflow history.")
	s.Command.Run = func(c *cobra.Command, args []string) {
		if err := s.run(cctx, args); err != nil {
			cctx.Options.Fail(err)
		}
	}
	return &s
}

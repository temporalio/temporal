package tdbg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/server/tools/tdbg/printer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CommandContext struct {
	// This context is closed on interrupt
	context.Context
	Options                   CommandOptions
	DeprecatedEnvConfigValues map[string]map[string]string
	FlagsWithEnvVars          []*pflag.Flag

	// These values may not be available until after pre-run of main command
	Printer               *printer.Printer
	Logger                *slog.Logger
	JSONOutput            bool
	JSONShorthandPayloads bool

	// Is set to true if any command actually started running. This is a hack to workaround the fact
	// that cobra does not properly exit nonzero if an unknown command/subcommand is given.
	ActuallyRanCommand bool

	// Root/current command only set inside of pre-run
	RootCommand    *TdbgCommand
	CurrentCommand *cobra.Command
}
type CommandOptions struct {
	// If empty, assumed to be os.Args[1:]
	Args []string

	// These three fields below default to OS values
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// Defaults to logging error then os.Exit(1)
	Fail func(error)

	AdditionalClientGRPCDialOptions []grpc.DialOption
	ClientConnectTimeout            time.Duration
}

// Execute runs the Temporal CLI with the given context and options. This
// intentionally does not return an error but rather invokes Fail on the
// options.
func Execute(ctx context.Context, options CommandOptions) {
	// Create context and run. We always get a context and cancel func back even
	// if an error was returned. This is so we can use the context to print an
	// error message using the appropriate Fail() method, regardless of why the
	// failure occurred.
	//
	// (In most cases, an error here likely means a problem with the user's env
	// config file, or some other issue in their environment.)
	cctx, cancel, err := NewCommandContext(ctx, options)
	defer cancel()

	if err == nil {
		// We have a context; let's actually run the command.
		cmd := NewTdbgCommand(cctx)
		cmd.Command.SetArgs(cctx.Options.Args)
		err = cmd.Command.ExecuteContext(cctx)
	}

	if err != nil {
		// Either we failed to create the context, OR the command itself failed.
		// Either way, we need to print an error message.
		cctx.Options.Fail(err)
	}

	// If no command ever actually got run, exit nonzero with an error.  This is
	// an ugly hack to make sure that iff the user explicitly asked for help, we
	// exit with a zero error code.  (The other situation in which help is
	// printed is when the user invokes an unknown command--we still want a
	// non-zero exit in that case.)  We should revisit this if/when the
	// following Cobra issues get fixed:
	//
	// - https://github.com/spf13/cobra/issues/1156
	// - https://github.com/spf13/cobra/issues/706
	if !cctx.ActuallyRanCommand {
		zeroExitArgs := []string{"--help", "-h", "--version", "-v", "help"}
		if slices.ContainsFunc(cctx.Options.Args, func(a string) bool {
			return slices.Contains(zeroExitArgs, a)
		}) {
			return
		}
		cctx.Options.Fail(fmt.Errorf("unknown command"))
	}
}

// NewCommandContext creates a CommandContext for use by the rest of the CLI.
// Among other things, this parses the env config file and modifies
// options/flags according to the parameters set there.
//
// A CommandContext and CancelFunc are always returned, even in the event of an
// error; this is so the CommandContext can be used to print an appropriate
// error message.
func NewCommandContext(ctx context.Context, options CommandOptions) (*CommandContext, context.CancelFunc, error) {
	cctx := &CommandContext{Context: ctx, Options: options}
	if err := cctx.preprocessOptions(); err != nil {
		return cctx, func() {}, err
	}

	// Setup interrupt handler
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	cctx.Context = ctx
	return cctx, stop, nil
}

func (c *CommandContext) preprocessOptions() error {
	if len(c.Options.Args) == 0 {
		c.Options.Args = os.Args[1:]
	}
	if c.Options.Stdin == nil {
		c.Options.Stdin = os.Stdin
	}
	if c.Options.Stdout == nil {
		c.Options.Stdout = os.Stdout
	}
	if c.Options.Stderr == nil {
		c.Options.Stderr = os.Stderr
	}

	// Setup default fail callback
	if c.Options.Fail == nil {
		c.Options.Fail = func(err error) {
			// If context is closed, say that the program was interrupted and ignore
			// the actual error
			if c.Err() != nil {
				err = fmt.Errorf("program interrupted")
			}
			if c.Logger != nil {
				c.Logger.Error(err.Error())
			} else {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(1)
		}
	}
	return nil
}

func updateOptionsFromEnv(c *CommandContext) {
}

const flagEnvVarAnnotation = "__temporal_env_var"

func (c *CommandContext) BindFlagEnvVar(flag *pflag.Flag, envVar string) {
	if flag.Annotations == nil {
		flag.Annotations = map[string][]string{}
	}
	flag.Annotations[flagEnvVarAnnotation] = []string{envVar}
	c.FlagsWithEnvVars = append(c.FlagsWithEnvVars, flag)
}

func (c *CommandContext) MarshalFriendlyJSONPayloads(m *common.Payloads) (json.RawMessage, error) {
	if m == nil {
		return []byte("null"), nil
	}
	// Use one if there's one, otherwise just serialize whole thing
	if p := m.GetPayloads(); len(p) == 1 {
		return c.MarshalProtoJSON(p[0])
	}
	return c.MarshalProtoJSON(m)
}

// Starts with newline
func (c *CommandContext) MarshalFriendlyFailureBodyText(f *failure.Failure, indent string) (s string) {
	for f != nil {
		s += "\n" + indent + "Message: " + f.Message
		if f.StackTrace != "" {
			s += "\n" + indent + "StackTrace:\n" + indent + "    " +
				strings.Join(strings.Split(f.StackTrace, "\n"), "\n"+indent+"    ")
		}
		if f = f.Cause; f != nil {
			s += "\n" + indent + "Cause:"
			indent += "    "
		}
	}
	return
}

// Takes payload shorthand into account, can use
// MarshalProtoJSONNoPayloadShorthand if needed
func (c *CommandContext) MarshalProtoJSON(m proto.Message) ([]byte, error) {
	return c.MarshalProtoJSONWithOptions(m, c.JSONShorthandPayloads)
}

func (c *CommandContext) MarshalProtoJSONWithOptions(m proto.Message, jsonShorthandPayloads bool) ([]byte, error) {
	opts := temporalproto.CustomJSONMarshalOptions{Indent: c.Printer.JSONIndent}
	if jsonShorthandPayloads {
		opts.Metadata = map[string]any{common.EnablePayloadShorthandMetadataKey: true}
	}
	return opts.Marshal(m)
}

func (c *CommandContext) UnmarshalProtoJSON(b []byte, m proto.Message) error {
	return UnmarshalProtoJSONWithOptions(b, m, c.JSONShorthandPayloads)
}

func UnmarshalProtoJSONWithOptions(b []byte, m proto.Message, jsonShorthandPayloads bool) error {
	opts := temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}
	if jsonShorthandPayloads {
		opts.Metadata = map[string]any{common.EnablePayloadShorthandMetadataKey: true}
	}
	return opts.Unmarshal(b, m)
}

// Set flag values from environment file & variables. Returns a callback to log anything interesting
// since logging will not yet be initialized when this runs.
func (c *CommandContext) populateFlagsFromEnv(flags *pflag.FlagSet) (func(*slog.Logger), error) {
	if flags == nil {
		return func(logger *slog.Logger) {}, nil
	}
	var logCalls []func(*slog.Logger)
	var flagErr error
	flags.VisitAll(func(flag *pflag.Flag) {
		// If the flag was already changed by the user, we don't overwrite
		if flagErr != nil || flag.Changed {
			return
		}
		// // Env config first, then environ
		// if v, ok := c.DeprecatedEnvConfigValues[c.Options.DeprecatedEnvConfig.EnvConfigName][flag.Name]; ok {
		// 	if err := flag.Value.Set(v); err != nil {
		// 		flagErr = fmt.Errorf("failed setting flag %v from config with value %v: %w", flag.Name, v, err)
		// 		return
		// 	}
		// 	flag.Changed = true
		// }
		// if anns := flag.Annotations[flagEnvVarAnnotation]; len(anns) == 1 {
		// 	if envVal, _ := c.Options.EnvLookup.LookupEnv(anns[0]); envVal != "" {
		// 		if err := flag.Value.Set(envVal); err != nil {
		// 			flagErr = fmt.Errorf("failed setting flag %v with env name %v and value %v: %w",
		// 				flag.Name, anns[0], envVal, err)
		// 			return
		// 		}
		// 		if flag.Changed {
		// 			logCalls = append(logCalls, func(l *slog.Logger) {
		// 				l.Info("Env var overrode --env setting", "env_var", anns[0], "flag", flag.Name)
		// 			})
		// 		}
		// 		flag.Changed = true
		// 	}
		// }
	})
	logFn := func(logger *slog.Logger) {
		for _, call := range logCalls {
			call(logger)
		}
	}
	return logFn, flagErr
}

func aliasNormalizer(aliases map[string]string) func(f *pflag.FlagSet, name string) pflag.NormalizedName {
	return func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		if actual := aliases[name]; actual != "" {
			name = actual
		}
		return pflag.NormalizedName(name)
	}
}

var buildInfo string

func VersionString() string {
	// To add build-time information to the version string, use
	// go build -ldflags "-X github.com/temporalio/cli/temporalcli.buildInfo=<MyString>"
	var bi = buildInfo
	if bi != "" {
		bi = fmt.Sprintf(", %s", bi)
	}
	return fmt.Sprintf("%s ", "TODO")
}
func (c *TdbgCommand) initCommand(cctx *CommandContext) {
	c.Command.Version = "TODO"
	// Unfortunately color is a global option, so we can set in pre-run but we
	// must unset in post-run
	origNoColor := color.NoColor
	c.Command.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Set command
		cctx.CurrentCommand = cmd
		// Populate environ. We will make the error return here which will cause
		// usage to be printed.
		logCalls, err := cctx.populateFlagsFromEnv(cmd.Flags())
		if err != nil {
			return err
		}

		// Default color.NoColor global is equivalent to "auto" so only override if
		// never or always
		if c.Color.Value == "never" || c.Color.Value == "always" {
			color.NoColor = c.Color.Value == "never"
		}

		res := c.preRun(cctx)

		logCalls(cctx.Logger)

		// Always disable color if JSON output is on (must be run after preRun so JSONOutput is set)
		if cctx.JSONOutput {
			color.NoColor = true
		}
		cctx.ActuallyRanCommand = true

		// if cctx.Options.DeprecatedEnvConfig.EnvConfigName != "default" {
		// 	if _, ok := cctx.DeprecatedEnvConfigValues[cctx.Options.DeprecatedEnvConfig.EnvConfigName]; !ok {
		// 		if _, ok := cmd.Annotations["ignoresMissingEnv"]; !ok {
		// 			// stfu about help output
		// 			cmd.SilenceErrors = true
		// 			cmd.SilenceUsage = true
		// 			return fmt.Errorf("environment %q not found", cctx.Options.DeprecatedEnvConfig.EnvConfigName)
		// 		}
		// 	}
		// }
		return res
	}
	c.Command.PersistentPostRun = func(*cobra.Command, []string) {
		color.NoColor = origNoColor
	}
}

func (c *TdbgCommand) preRun(cctx *CommandContext) error {
	// Set this command as the root
	cctx.RootCommand = c
	return nil
}

func newNopLogger() *slog.Logger { return slog.New(discardLogHandler{}) }

type discardLogHandler struct{}

func (discardLogHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardLogHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardLogHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardLogHandler) WithGroup(string) slog.Handler           { return d }

type nopWriter struct{}

func (nopWriter) Write(b []byte) (int, error) { return len(b), nil }

func (c TdbgDecodeBase64Command) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDecodeProtoCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDlqJobCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDlqMergeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDlqListCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgDlqPurgeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgDlqReadCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgHistoryHostCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgMembershipCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgShardCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgTaskqueueDescribeTaskQueuePartitionCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgTaskqueueForceUnloadTaskQueuePartitionCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgTaskqueueListTasksCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowDeleteCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowDescribeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowImportCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowRebuildCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowRefreshTasksCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowReplicateCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgWorkflowShowCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgHistoryHostDescribeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDecodeTaskCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgMembershipListDbCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgMembershipListGossipCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgShardCloseShardCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgShardListTasksCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgShardRemoveTaskCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgShardDescribeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}
func (c TdbgDlqJobCancelCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

func (c TdbgDlqJobDescribeCommand) run(cctx *CommandContext, args []string) error {
	return nil
}

package tdbg

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/multierr"
)

func getCommands(
	clientFactory ClientFactory,
	dlqServiceProvider *DLQServiceProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	prompterFactory PrompterFactory,
	taskBlobEncoder TaskBlobEncoder,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:        "execution",
			Aliases:     []string{"e", "w", "workflow"},
			Usage:       "Run admin operation on an execution (workflow)",
			Subcommands: newAdminExecutionCommands(clientFactory, prompterFactory),
		},
		{
			Name:        "shard",
			Aliases:     []string{"s"},
			Usage:       "Run admin operation on specific shard",
			Subcommands: newAdminShardManagementCommands(clientFactory, taskCategoryRegistry),
		},
		{
			Name:        "history-host",
			Aliases:     []string{"hh"},
			Usage:       "Run admin operation on history host",
			Subcommands: newAdminHistoryHostCommands(clientFactory),
		},
		{
			Name:        "taskqueue",
			Aliases:     []string{"tq"},
			Usage:       "Run admin operation on taskQueue",
			Subcommands: newAdminTaskQueueCommands(clientFactory),
		},
		{
			Name:        "membership",
			Aliases:     []string{"m"},
			Usage:       "Run admin operation on membership",
			Subcommands: newAdminMembershipCommands(clientFactory),
		},
		{
			Name:        "dlq",
			Usage:       "Run admin operation on DLQ",
			Subcommands: newAdminDLQCommands(dlqServiceProvider, taskCategoryRegistry),
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagDLQVersion,
					Usage: "Version of DLQ to manage, options: v1, v2",
					Value: "v2",
				},
			},
		},
		{
			Name:        "schedule",
			Aliases:     []string{"sch"},
			Usage:       "Run admin operation on a schedule",
			Subcommands: newAdminScheduleCommands(clientFactory),
		},
		{
			Name:        "decode",
			Usage:       "Decode payload",
			Subcommands: newDecodeCommands(taskBlobEncoder),
		},
	}
}

func newAdminExecutionCommands(clientFactory ClientFactory, prompterFactory PrompterFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "import",
			Usage: "import workflow history to database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:  FlagInputFilename,
					Usage: "input file",
				}},
			Action: func(c *cli.Context) error {
				return AdminImportWorkflow(c, clientFactory)
			},
		},
		{
			Name:  "show",
			Usage: "show workflow history from database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.Int64Flag{
					Name:  FlagMinEventID,
					Usage: "Minimum event ID to be included in the history",
				},
				&cli.Int64Flag{
					Name:  FlagMaxEventID,
					Usage: "Maximum event ID to be included in the history",
					Value: 1<<63 - 1,
				},
				&cli.Int64Flag{
					Name:  FlagMinEventVersion,
					Usage: "Start event version to be included in the history",
				},
				&cli.Int64Flag{
					Name:  FlagMaxEventVersion,
					Usage: "End event version to be included in the history",
				},
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "output file",
				},
				&cli.BoolFlag{
					Name:  FlagDecode,
					Usage: "Automatically decode payload data to JSON",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminShowWorkflow(c, clientFactory)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe internal information of Temporal execution",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagBusinessID,
					Aliases: FlagBusinessIDAlias,
					Usage:   "Business ID (Workflow ID)",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID (optional, uses latest if not specified)",
				},
				&cli.StringFlag{
					Name:        FlagArchetype,
					Usage:       "Fully qualified archetype name of the execution",
					DefaultText: chasm.WorkflowArchetype,
				},
				&cli.UintFlag{
					Name:  FlagArchetypeID,
					Usage: "Archetype ID (optional, overrides --archetype if specified)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeExecution(c, clientFactory)
			},
		},
		{
			Name:    "refresh-tasks",
			Aliases: []string{"rt"},
			Usage:   "Refreshes all the tasks of a workflow",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:        FlagArchetype,
					Usage:       "Fully qualified archetype name of the execution",
					DefaultText: chasm.WorkflowArchetype,
				},
				&cli.UintFlag{
					Name:  FlagArchetypeID,
					Usage: "Archetype ID (optional, overrides --archetype if specified)",
				},
				&cli.StringFlag{
					Name:  FlagVisibilityQuery,
					Usage: "Visibility query to select workflows",
				},
				&cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason for starting the batch job",
				},
				&cli.StringFlag{
					Name:  FlagJobID,
					Usage: "Optional job ID (auto-generated if not provided)",
				},
			},
			Action: func(c *cli.Context) error {
				return adminRefreshWorkflowTasks(c, clientFactory, prompterFactory(c))
			},
		},
		{
			Name:    "rebuild",
			Aliases: []string{},
			Usage:   "Rebuild a workflow mutable state using persisted history events",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminRebuildMutableState(c, clientFactory)
			},
		},
		{
			Name:    "replicate",
			Aliases: []string{},
			Usage:   "Force replicate a workflow by generating replication tasks",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:        FlagArchetype,
					Usage:       "Fully qualified archetype name of the execution",
					DefaultText: chasm.WorkflowArchetype,
				},
				&cli.UintFlag{
					Name:  FlagArchetypeID,
					Usage: "Archetype ID (optional, overrides --archetype if specified)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminReplicateWorkflow(c, clientFactory)
			},
		},
		{
			Name:    "delete",
			Aliases: []string{"del"},
			Usage:   "Delete current workflow execution and the mutableState record",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:        FlagArchetype,
					Usage:       "Fully qualified archetype name of the execution",
					DefaultText: chasm.WorkflowArchetype,
				},
				&cli.UintFlag{
					Name:  FlagArchetypeID,
					Usage: "Archetype ID (optional, overrides --archetype if specified)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDeleteWorkflow(c, clientFactory, prompterFactory(c))
			},
		},
	}
}

func newAdminScheduleCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "migrate",
			Usage: "Migrate a schedule between V1 (workflow-backed) and V2 (CHASM)",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagScheduleID,
					Aliases: FlagScheduleIDAlias,
					Usage:   "Schedule ID (single-schedule mode)",
				},
				&cli.StringFlag{
					Name: FlagTarget,
					// Not marked Required here: it is validated (and required) by
					// parseMigrateTarget when the migrate action itself runs. Marking it
					// Required at the CLI level would also apply to the "status" subcommand
					// below, which has no use for --target.
					Usage: "Target scheduler implementation: chasm, workflow",
				},
				&cli.BoolFlag{
					Name: FlagFromVisibility,
					Usage: "Select schedules from visibility instead of --schedule-id, scoped to --namespace. " +
						"The default query is chosen from --target: migrating to chasm selects running V1 schedules, " +
						"migrating to workflow selects running V2 schedules. Override with --query",
				},
				&cli.StringFlag{
					Name: FlagVisibilityQuery,
					Usage: "Visibility query used with --from-visibility, overriding the target-based default. The defaults are:\n" +
						"\tV1 (workflow-backed): TemporalNamespaceDivision = 'TemporalScheduler' AND ExecutionStatus = 'Running'\n" +
						"\tV2 (CHASM):           TemporalNamespaceDivision = '<scheduler-archetype-id>' AND ExecutionStatus = 'Running'",
				},
				&cli.BoolFlag{
					Name:  FlagExecute,
					Usage: "Perform the migration. Without this flag, --from-visibility and stdin modes only print what they would do (dry-run)",
				},
				&cli.IntFlag{
					Name:  FlagWorkers,
					Value: defaultMigrateWorkers,
					Usage: "Number of concurrent workers migrating schedules in --from-visibility and stdin modes",
				},
				&cli.StringFlag{
					Name:  FlagOutputLog,
					Usage: "Path to write a structured (JSON lines) log of each migration result in --from-visibility and stdin modes",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminMigrateSchedule(c, clientFactory)
			},
			Subcommands: []*cli.Command{
				{
					Name:  "status",
					Usage: "Show counts of V1 (workflow-backed) and V2 (CHASM) schedules in --namespace, from visibility",
					Action: func(c *cli.Context) error {
						return AdminScheduleStatus(c, clientFactory)
					},
				},
			},
		},
		{
			Name:  "audit",
			Usage: "Audit a namespace's schedules for missed runs in a time window",
			Description: `Lists every schedule in the target namespace(s), computes the nominal times each spec should have
produced in the audit window, and classifies each against actual workflow executions found in visibility.
Designed to answer: did the scheduler start workflows when it should have, during a specific time range?

Input and output both stream: schedules are processed as they page in (per-schedule describe -> visibility query ->
classify), so a namespace with millions of schedules runs with bounded memory and rows begin appearing immediately.

OUTPUT FORMAT
  JSONL streamed to stdout: one self-contained JSON object per flagged schedule (redirect to a file if desired).
  Rows are emitted in completion order (NOT sorted) as each schedule finishes; pipe through 'sort' or 'jq -s' if you
  need a stable order. Each object is the full analysis result -- identity, audit window, every input used, and the
  classification -- so a verdict can be inspected or replayed offline. Fields:
    namespace, schedule_id, workflow_type
    window {start, end}            the audited time range
    overlap_policy                 the schedule's overlap policy (short name, e.g. BufferAll)
    overlap_class                  how that policy treats overlaps, which drives classification:
                                     drops       Skip / BufferOne -- overlapping fires can be dropped for good
                                     delays      BufferAll / CancelOther / TerminateOther -- overlaps run later
                                     concurrent  AllowAll -- overlaps run immediately
    catchup_window                 the schedule's configured catchupWindow (Go duration string, e.g. "1m0s")
    create_time, update_time       schedule create/update times (null if unknown)
    expected, actual, matched      scheduled times; unique workflows observed; scheduled times matched to a workflow
    missed                         total unmatched scheduled times
    counts {real_miss, skip_overlap, inconclusive_schedule_changed}
    misses [{nominal, category}]   every unmatched scheduled time and why:
                                     real_miss                      no workflow and the policy doesn't justify a skip
                                                                    (a delays/concurrent policy, or a drops policy with
                                                                    nothing running). Warrants investigation.
                                     skip_overlap                   a drops policy legitimately skipped because a prior
                                                                    workflow was still running
                                     inconclusive_schedule_changed  spec was modified DURING the window; can't be judged
    scheduled_times []             every nominal time the spec produced in the window
    observed [{workflow_id, run_id, nominal, start, close, status}]  every workflow seen in visibility
    delays [{workflow_id, nominal, actual, desired, start, ...}]  per started action, how late it was:
                                     nominal          pre-jitter scheduled time (N)
                                     actual           jittered intended fire time (A = N + deterministic jitter)
                                     desired          when it became eligible (D; a prior action's close time if a
                                                      delaying overlap policy held it, else = actual)
                                     start            actual workflow start (S)
                                     jitter_offset  A - N (intended load spreading)
                                     overlap_wait   max(0, D - A) (held behind a prior action by the overlap policy)
                                     dispatch_delay S - D -- system was slow to start it once eligible
                                     e2e_delay      S - A (total delay from the intended fire time)
                                   (durations are Go duration strings, e.g. "5m0s")

CAVEATS AND LIMITATIONS
  Retention: when --start is older than the namespace's retention boundary (now - retention), the window start is
    clamped to that boundary and a warning is logged to stderr. Visibility purges closed workflows after retention, so
    querying past it would surface purged data as false-positive real_miss; clamping audits only the portion that still
    has data.

  Schedule modified during window: marked inconclusive_schedule_changed. The audit can't compute the historical spec,
    so the schedule's times are marked inconclusive rather than producing untrustworthy classifications.

  Paused / exhausted schedules: dropped from analysis (their spec evaluates to scheduled times but the scheduler
    won't start them).

  Catchup window: a schedule with a tight catchupWindow (e.g. 10s) will lose actions if the scheduler is briefly
    unavailable. The audit reports such losses as real_miss; catchup_window lets users distinguish "true sustained
    outage" from "brief blip + tight catchup".

  Catchup under overlap=SKIP after a brief outage: when the V1 scheduler resumes with multiple queued actions, it
    dispatches only the oldest and discards the rest. The audit reports the discarded actions as real_miss. A burst of
    real_miss across many schedules within minutes typically indicates this behavior. To confirm, the user must:
    (1) ListWorkflowExecutions with WorkflowId='temporal-sys-scheduler:<schedule_id>' and
    TemporalNamespaceDivision='TemporalScheduler' to find the scheduler workflow run active during the window;
    (2) GetWorkflowExecutionHistory on that run and look for WORKFLOW_TASK_TIMED_OUT events with escalating attempt
    numbers, followed by a multi-minute gap, then a single WORKFLOW_TASK_COMPLETED that resumes normal cadence;
    (3) compare the identity field on WORKFLOW_TASK_STARTED across affected schedules. A shared worker identity points
    to a single sick worker pod; distinct identities point to a broader frontend/matching/persistence issue.

INPUT STREAM (for --file / stdin)
  A JSONL stream of audit targets, one JSON object per line; schedule_id is optional (omit to audit the whole
  namespace). Examples:
    {"namespace":"my-namespace"}
    {"namespace":"orders-prod","schedule_id":"daily-cleanup-schedule"}
    {"namespace":"analytics-staging","schedule_id":"my-schedule"}

PRECEDENCE
  With a --file/stdin stream, the stream is the source of targets and the flags are constraints: every streamed
  target must agree with --namespace and --schedule-id when those are set, otherwise the run errors. Pass --namespace
  to guarantee the audit stays within a single namespace.
  With no stream (an interactive terminal and no --file), the flags define the target directly: --namespace alone
  audits that whole namespace; --namespace + --schedule-id audits that one schedule.

TIME WINDOW
  --start / --end take a duration before now (e.g. 24h, 3d, 90m, 0s); "d" means exactly 24h. --start-time / --end-time
  take an absolute RFC3339 timestamp instead. --start is required (or --start-time); --end defaults to now. The
  duration and timestamp forms of a bound are mutually exclusive.

EXAMPLES
  Last 24 hours of a namespace, save to a file:
    tdbg schedule audit --namespace my-ns --start 24h > audit.jsonl

  Last 3 days ending 6 hours ago:
    tdbg schedule audit --namespace my-ns --start 3d --end 6h > audit.jsonl

  Absolute window from a file of targets:
    tdbg schedule audit -f ./targets.jsonl \
      --start-time 2026-05-01T19:30:00Z --end-time 2026-05-02T10:00:00Z > audit.jsonl

  Pipe a JSONL target stream from stdin (jq, psql, awk, etc.):
    cat ./targets.jsonl | tdbg schedule audit --start 2d

  Single schedule deep-dive over an absolute window:
    tdbg schedule audit --namespace my-ns --schedule-id my-schedule \
      --start-time 2026-05-19T18:00:00Z --end-time 2026-05-19T22:00:00Z

  Pipe stdout through jq (e.g. only schedules with real misses):
    tdbg schedule audit -f ./targets.jsonl --start 1d | jq 'select(.counts.real_miss > 0)'`,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagNamespace,
					Aliases: FlagNamespaceAlias,
					Usage: "Audit this namespace. With no stream, audits the whole namespace. With a --file/stdin " +
						"stream, it is a constraint: every streamed target must be in this namespace or the run errors, " +
						"guaranteeing the audit stays within a single namespace.",
				},
				&cli.StringFlag{
					Name:    FlagFile,
					Aliases: FlagFileAlias,
					Usage: "Path to a JSONL stream of audit targets, one JSON object per line as " +
						`'{"namespace":"...","schedule_id":"..."}' (schedule_id optional). Use '-' or omit to read ` +
						"from stdin. Targets stream in as they are read.",
				},
				&cli.StringFlag{
					Name:    FlagScheduleID,
					Aliases: FlagScheduleIDAlias,
					Usage: "Audit only this schedule within --namespace. With no stream, audits just this schedule; " +
						"with a stream, it is a constraint: every streamed target's schedule_id must match or the run errors.",
				},
				&cli.StringFlag{
					Name: FlagStart,
					Usage: "Window start as a duration before now (e.g. 24h, 3d). Required unless --start-time is given; " +
						"mutually exclusive with it.",
				},
				&cli.StringFlag{
					Name:  FlagStartTime,
					Usage: "Window start as an absolute RFC3339 timestamp. Mutually exclusive with --start.",
				},
				&cli.StringFlag{
					Name: FlagEnd,
					Usage: "Window end as a duration before now (e.g. 1h, 0s). Defaults to now; mutually exclusive with " +
						"--end-time.",
				},
				&cli.StringFlag{
					Name:  FlagEndTime,
					Usage: "Window end as an absolute RFC3339 timestamp. Defaults to now; mutually exclusive with --end.",
				},
				&cli.IntFlag{
					Name:  FlagConcurrency,
					Usage: "How many namespaces to audit at once (the namespace worker-pool size).",
					Value: 10,
				},
				&cli.IntFlag{
					Name:  FlagRPS,
					Usage: "Max API calls per second within a single namespace (describe, list, and visibility queries).",
					Value: 10,
				},
				&cli.DurationFlag{
					Name: FlagDelayThreshold,
					Usage: "Also flag a schedule (even with no missed runs) when a started action's dispatch delay " +
						"reaches this -- surfaces schedules the system was slow to start. 0 flags on missed runs only.",
					Value: 0,
				},
			},
			Action: func(c *cli.Context) error {
				return AdminAuditSchedules(c, clientFactory)
			},
		},
	}
}

func newAdminShardManagementCommands(clientFactory ClientFactory, taskCategoryRegistry tasks.TaskCategoryRegistry) []*cli.Command {
	// There are two different categories for the task type, and they have slightly
	// different semantics. The first is the task category for the list-tasks command,
	// which is required and does not have a default. The second is the task category
	// for the remove-task command, which is optional and defaults to transfer.
	taskCategoryFlag := getTaskCategoryFlag(taskCategoryRegistry)
	return []*cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe shard by ID",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "The ID of the shard to describe",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeShard(c, clientFactory)
			},
		},
		{
			Name:  "list-tasks",
			Usage: "List tasks for given shard ID and task category",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  FlagMore,
					Usage: "List more pages, default is to list one page of default page size 10",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Value: defaultPageSize,
					Usage: "Result page size",
				},
				&cli.IntFlag{
					Name:     FlagShardID,
					Usage:    "The ID of the shard",
					Required: true,
				},
				taskCategoryFlag,
				&cli.Int64Flag{
					Name:  FlagMinTaskID,
					Usage: "Inclusive min taskID. Optional for transfer, replication, visibility tasks. Can't be specified for timer task",
				},
				&cli.Int64Flag{
					Name:  FlagMaxTaskID,
					Usage: "Exclusive max taskID. Required for transfer, replication, visibility tasks. Can't be specified for timer task",
				},
				&cli.StringFlag{
					Name: FlagMinVisibilityTimestamp,
					Usage: "Inclusive min task fire timestamp. Optional for timer task. Can't be specified for transfer, replication, visibility tasks. " +
						"Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.StringFlag{
					Name: FlagMaxVisibilityTimestamp,
					Usage: "Exclusive max task fire timestamp. Required for timer task. Can't be specified for transfer, replication, visibility tasks. " +
						"Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Value: true,
					Usage: "Print in raw json format",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListShardTasks(c, clientFactory, taskCategoryRegistry)
			},
		},
		{
			Name:  "close-shard",
			Usage: "close a shard given a shard id",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "ShardId for the temporal cluster to manage",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminShardManagement(c, clientFactory)
			},
		},
		{
			Name:    "remove-task",
			Aliases: []string{"rmtk"},
			Usage:   "remove a task based on shardId, task category, taskId, and task visibility timestamp",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:     FlagShardID,
					Usage:    "shardId",
					Required: true,
				},
				&cli.Int64Flag{
					Name:     FlagTaskID,
					Usage:    "taskId",
					Required: true,
				},
				taskCategoryFlag,
				&cli.Int64Flag{
					Name:  FlagTaskVisibilityTimestamp,
					Usage: "task visibility timestamp in nano (required for removing timer task)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminRemoveTask(c, clientFactory, taskCategoryRegistry)
			},
		},
	}
}

func getTaskCategoryFlag(taskCategoryRegistry tasks.TaskCategoryRegistry) *cli.StringFlag {
	categories := taskCategoryRegistry.GetCategories()
	options := make([]string, 0, len(categories))
	for _, category := range categories {
		options = append(options, category.Name())
	}
	flag := &cli.StringFlag{
		Name:     FlagTaskCategory,
		Usage:    "Task category: " + strings.Join(options, ", "),
		Required: true,
	}
	return flag
}

func newAdminMembershipCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "list-gossip",
			Usage: "List ringpop membership items",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagClusterMembershipRole,
					Value: "all",
					Usage: "Membership role filter: all (default), frontend, history, matching, worker",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListGossipMembers(c, clientFactory)
			},
		},
		{
			Name:  "list-db",
			Usage: "List cluster membership items",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagHeartbeatedWithin,
					Value: "15m",
					Usage: "Filter by last heartbeat date time. Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.StringFlag{
					Name:  FlagClusterMembershipRole,
					Value: "all",
					Usage: "Membership role filter: all (default), frontend, history, matching, worker",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListClusterMembers(c, clientFactory)
			},
		},
	}
}

func newAdminHistoryHostCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe internal information of history host",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:  FlagHistoryAddress,
					Usage: "History Host address(IP:PORT)",
				},
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "ShardId",
				},
				&cli.BoolFlag{
					Name:  FlagPrintFullyDetail,
					Usage: "Print fully detail",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeHistoryHost(c, clientFactory)
			},
		},
		{
			Name:  "get-shardid",
			Usage: "Get shardId for a namespaceId and workflowId combination",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagNamespaceID,
					Usage: "NamespaceId",
				},
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.IntFlag{
					Name:  FlagNumberOfShards,
					Usage: "NumberOfShards for the temporal cluster(see config for numHistoryShards)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminGetShardID(c)
			},
		},
	}
}

func newAdminTaskQueueCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "list-tasks",
			Usage: "List tasks of a task queue. Use --fair to list fairness tasks.",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  FlagMore,
					Usage: "List more pages, default is to list one page of default page size 10",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Value: 10,
					Usage: "Result page size",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Value: "activity",
					Usage: "Task Queue type: activity, workflow",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueue,
					Usage: "Task Queue name",
				},
				&cli.Int64Flag{
					Name:  FlagMinTaskID,
					Usage: "Minimum task ID",
					Value: -12346, // include default task id
				},
				&cli.Int64Flag{
					Name:  FlagMaxTaskID,
					Usage: "Maximum task ID",
				},
				&cli.IntFlag{
					Name:  FlagSubqueue,
					Usage: "Subqueue to query",
					Value: 0,
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Usage: "Print in raw json format",
				},
				&cli.BoolFlag{
					Name:  FlagFair,
					Usage: "Query fairness tasks",
				},
				&cli.Int64Flag{
					Name:  FlagMinPass,
					Usage: "Minimum pass (fairness task only)",
					Value: 1,
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListTaskQueueTasks(c, clientFactory)
			},
		},
		{
			Name:  "describe-task-queue-partition",
			Usage: "Describe information related to a task queue partition",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagNamespaceID,
					Usage: "NamespaceId",
					Value: "default",
				},
				&cli.StringFlag{
					Name:     FlagTaskQueue,
					Usage:    "Task Queue name",
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Value: "TASK_QUEUE_TYPE_WORKFLOW",
					Usage: "Task Queue type: activity, workflow, nexus (experimental)",
				},
				&cli.Int64Flag{
					Name:  FlagPartitionID,
					Usage: "Partition ID",
					Value: 0,
				},
				&cli.StringFlag{
					Name:  FlagStickyName,
					Usage: "Sticky Name for a task queue partition, if present",
					Value: "",
				},
				&cli.StringSliceFlag{
					Name:  FlagBuildIDs,
					Value: &cli.StringSlice{},
					Usage: "Build IDs",
				},
				&cli.BoolFlag{
					Name:  FlagUnversioned,
					Usage: "Unversioned task queue partition",
					Value: true,
				},
				&cli.BoolFlag{
					Name:  FlagAllActive,
					Usage: "All active task queue versions",
					Value: true,
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeTaskQueuePartition(c, clientFactory)
			},
		},
		{
			Name:  "force-unload-task-queue-partition",
			Usage: "Forcefully unload a task queue partition",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagNamespaceID,
					Usage: "NamespaceId",
					Value: "default",
				},
				&cli.StringFlag{
					Name:     FlagTaskQueue,
					Usage:    "Task Queue name",
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Value: "TASK_QUEUE_TYPE_WORKFLOW",
					Usage: "Task Queue type: activity, workflow, nexus (experimental)",
				},
				&cli.Int64Flag{
					Name:  FlagPartitionID,
					Usage: "Partition ID",
					Value: 0,
				},
				&cli.StringFlag{
					Name:  FlagStickyName,
					Usage: "Sticky Name for a task queue partition, if present",
					Value: "",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminForceUnloadTaskQueuePartition(c, clientFactory)
			},
		},
		{
			Name:  "get-user-data",
			Usage: "Get per-type user data stored for a task queue",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagNamespace,
					Usage:    "Namespace name",
					Required: true,
				},
				&cli.StringFlag{
					Name:     FlagTaskQueue,
					Usage:    "Task Queue name",
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Value: "TASK_QUEUE_TYPE_WORKFLOW",
					Usage: "Task Queue type: TASK_QUEUE_TYPE_WORKFLOW, TASK_QUEUE_TYPE_ACTIVITY, TASK_QUEUE_TYPE_NEXUS (default TASK_QUEUE_TYPE_WORKFLOW)",
				},
				&cli.Int64Flag{
					Name:  FlagPartitionID,
					Usage: "Partition ID to fetch user data from (default 0 = root partition)",
					Value: 0,
				},
			},
			Action: func(c *cli.Context) error {
				return AdminGetTaskQueueUserData(c, clientFactory)
			},
		},
	}
}

func newAdminDLQCommands(
	dlqServiceProvider *DLQServiceProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:    "read",
			Aliases: []string{"r"},
			Usage:   "Read DLQ Messages",
			Flags: append(
				getDLQFlags(taskCategoryRegistry),
				&cli.IntFlag{
					Name: FlagMaxMessageCount,
					Usage: fmt.Sprintf(
						"Max message size to fetch, defaults to %d for v2 and nothing for v1",
						dlqV2DefaultMaxMessageCount,
					),
				},
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "Output file to write to, if not provided output is written to stdout",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Usage: "Page size to use when reading messages from the DB, v2 only",
					Value: defaultPageSize,
				},
			),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.ReadMessages(c)
			},
		},
		{
			Name:    "purge",
			Aliases: []string{"p"},
			Usage:   "Delete DLQ messages with equal or smaller ids than the provided task id",
			Flags:   getDLQFlags(taskCategoryRegistry),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.PurgeMessages(c)
			},
		},
		{
			Name:        "merge",
			Aliases:     []string{"m"},
			Usage:       "Merge DLQ messages with equal or smaller ids than the provided task id",
			Description: "This command will delete messages after they've been re-enqueued if using v2.",
			Flags: append(getDLQFlags(taskCategoryRegistry),
				&cli.IntFlag{
					Name: FlagPageSize,
					Usage: "Batch size to use when purging messages from the DB, v2 only. Will use server default if " +
						"not provided.",
				},
			),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.MergeMessages(c)
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "List all DLQs, only for v2",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "Output file to write to, if not provided output is written to stdout",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Usage: "Page size to use when listing queues from the DB",
					Value: defaultPageSize,
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Usage: "Print in raw json format",
				},
			},
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.ListQueues(c)
			},
		},
		{
			Name:        "job",
			Usage:       "Run admin operation on DLQ Job",
			Subcommands: newAdminDLQJobCommands(dlqServiceProvider),
		},
	}
}

func newAdminDLQJobCommands(
	dlqServiceProvider *DLQServiceProvider,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:        "describe",
			Aliases:     []string{"d"},
			Usage:       "Get details of the DLQ job with provided job token",
			Description: "This command will get details of the DLQ job with provided job token if using v2",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagJobToken,
					Usage:    "Token of the DLQ job. This token will be printed in the output of merge and purge commands",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				ac := dlqServiceProvider.GetDLQJobService()
				return ac.DescribeJob(c)
			},
		},
		{
			Name:        "cancel",
			Aliases:     []string{"c"},
			Usage:       "Cancel the DLQ job with provided job token",
			Description: "This command will cancel the DLQ job with provided job token",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagJobToken,
					Usage:    "Token of the DLQ job. This token will be printed in the output of merge and purge commands",
					Required: true,
				},
				&cli.StringFlag{
					Name:     FlagReason,
					Usage:    "Reason for job cancellation",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				ac := dlqServiceProvider.GetDLQJobService()
				return ac.CancelJob(c)
			},
		},
	}
}

func getDLQFlags(taskCategoryRegistry tasks.TaskCategoryRegistry) []cli.Flag {
	categoriesString := getCategoriesList(taskCategoryRegistry)
	return []cli.Flag{
		&cli.StringFlag{
			Name: FlagDLQType,
			Usage: fmt.Sprintf(
				"Type of DLQ to manage, options: namespace, history for v1; %s for v2",
				categoriesString,
			),
		},
		&cli.StringFlag{
			Name:  FlagCluster,
			Usage: "Source cluster",
		},
		&cli.IntFlag{
			Name:  FlagShardID,
			Usage: "ShardId, v1 only",
		},
		&cli.IntFlag{
			Name: FlagLastMessageID,
			Usage: "The upper boundary of messages to operate on. If not provided, all messages will be operated on. " +
				"However, you will be prompted for confirmation unless the --yes flag is also provided.",
		},
		&cli.StringFlag{
			Name:  FlagTargetCluster,
			Usage: "Target cluster, v2 only. If not provided, current cluster is used.",
		},
	}
}

func newDecodeCommands(
	taskBlobEncoder TaskBlobEncoder,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "proto",
			Usage: "Decode proto payload",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagProtoType,
					Usage: "full name of proto type to decode to (i.e. temporal.server.api.persistence.v1.WorkflowExecutionInfo).",
				},
				&cli.StringFlag{
					Name:  FlagHexData,
					Usage: "data in hex format (i.e. 0x0a243462613036633466...).",
				},
				&cli.StringFlag{
					Name:  FlagHexFile,
					Usage: "file with data in hex format (i.e. 0x0a243462613036633466...).",
				},
				&cli.StringFlag{
					Name:  FlagBinaryFile,
					Usage: "file with data in binary format.",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDecodeProto(c)
			},
		},
		{
			Name:  "base64",
			Usage: "Decode base64 payload",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagBase64Data,
					Usage: "data in base64 format (i.e. anNvbi9wbGFpbg==).",
				},
				&cli.StringFlag{
					Name:  FlagBase64File,
					Usage: "file with data in base64 format (i.e. anNvbi9wbGFpbg==).",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDecodeBase64(c)
			},
		},
		{
			Name:  "task",
			Usage: "Decode a history task blob into a JSON message.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagBinaryFile,
					Usage:    "file with data in binary format.",
					Required: true,
				},
				&cli.IntFlag{
					Name:     FlagTaskCategoryID,
					Usage:    "Task category ID (see the history/tasks package)",
					Required: true,
				},
				&cli.StringFlag{
					Name:     FlagEncoding,
					Usage:    "Encoding type (see temporal.api.enums.v1.EncodingType)",
					Required: true,
				},
			},
			Action: func(c *cli.Context) (err error) {
				encoding := c.String(FlagEncoding)
				encodingType, err := enumspb.EncodingTypeFromString(encoding)
				if err != nil {
					return err
				}
				taskCategoryID := c.Int(FlagTaskCategoryID)
				file, err := os.Open(c.String(FlagBinaryFile))
				if err != nil {
					return fmt.Errorf("failed to open file: %w", err)
				}
				defer func() {
					err = multierr.Combine(err, file.Close())
				}()
				b, err := io.ReadAll(file)
				if err != nil {
					return fmt.Errorf("failed to read file: %w", err)
				}
				blob := commonpb.DataBlob{
					EncodingType: encodingType,
					Data:         b,
				}
				if err := taskBlobEncoder.Encode(c.App.Writer, taskCategoryID, &blob); err != nil {
					return fmt.Errorf("failed to decode task blob: %w", err)
				}
				return nil
			},
		},
	}
}

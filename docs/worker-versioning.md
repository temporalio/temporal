Worker Versioning
=================

**Note 1:** In this iteration of Worker Versioning we deprecated the Version Set concept 
and APIs related to it. If you are using old Worker Versioning APIs please migrate
to the new APIs using the process [outlined in a later section](#migrating-from-version-sets).

**Note 2:** Worker Versioning is still considered experimental and not recommended 
for production usage. Future breaking changes may be made if deemed necessary.

Worker Versioning simplifies the process of deploying changes to [Worker Programs](https://docs.temporal.io/workers#worker-program). 
It does this by letting you specify a Build ID for your Worker. Temporal Server
uses the Build ID to route each Workflow and/or Activity to a Worker instance that
can process it.

Worker Versioning guarantees that Workflow Executions started on a particular Build ID
will only be processed by Workers of the same Build ID, unless instructed otherwise 
(via [Redirect Rules](#redirect-rules)). With this guarantee, you can make any
([non-deterministic](https://docs.temporal.io/workflows#non-deterministic-change)) 
change to your Worker Programs freely and instruct Temporal Server to send new 
executions to the new Build ID and let old executions run on their old Build IDs to
completion.

When using Worker Versioning, you may need to run multiple versions of your Worker for
some time, until Workers of old Build IDs are not needed anymore (i.e. they are not
reachable by any current or future Workflow Execution). Temporal provides Reachability
API to help determine when a Worker version can be decommissioned. 

## When and why you should use Worker Versioning

The main reason to use this feature is that it frees you from having to worry about
nondeterministic changes. This is a significant gain, however it comes at the cost of
running multiple versions of Workers simultaneously.

For this reason, Worker Versioning is best suited for short-lived Workflows. Because 
the old Build ID is only needed to be kept for a short window during/after a deployment
until all open workflows belonging to it close.

For long-running Workflows, you would need to keep multiple versions of your Workers 
running for an extended duration, or avoid making any nondeterministic changes by using
[Patching](https://docs.temporal.io/workflows#patching) or other means.

There are also ways to break some long-running Workflows to shorter ones to benefit from
Worker Versioning.

## Worker Versioning Rules

Temporal Server allows you to manage routing of tasks to Build IDs via 
**Worker Versioning Rules**.

Worker Versioning rules and added to a given Task Queue. If your Worker Program contains 
multiple Workers polling multiple Task Queues, you would need to separately update the rules 
of each Task Queue.

There are two types of rules: _Build ID Assignment rules_ and _Compatible Build ID Redirect rules_.

### Assingment Rules
Assignment rules are used to assign a Build ID for a new execution when it starts. Their primary
use case is to specify the latest Build ID, but they have powerful features for gradual rollout
of a new Build ID.

Once a Build ID is assigned to a Workflow Execution, and it completes its first Workflow Task,
the workflow stays on the assigned Build ID regardless of changes in Assignment rules. This
eliminates the need for compatibility between versions when you only care about using the new
version for new Workflows and let existing Workflows finish in their own version.

Activities, Child Workflows and Continue-as-New executions have the option to inherit the 
Build ID of their parent/previous Workflow or use the latest Assignment rules to independently 
select a Build ID. This is specified by the parent/previous Workflow using VersioningIntent.

In absence of (applicable) Redirect rules the task will be dispatched to Workers 
of the Build ID determined by the Assignment rules (or inherited). Otherwise, the final 
Build ID will be determined by the Redirect rules.

When using Worker Versioning on a Task Queue, in the steady state,
there should typically be a single assignment rule to send all new executions
to the latest Build ID. Existence of at least one such "unconditional"
rule at all times is enforces by the system, unless the `force` flag is used
by the user when replacing/deleting these rules (for exceptional cases).

During a deployment, one or more additional rules can be added to assign a
subset of the tasks to a new Build ID based on a "ramp percentage".

When there are multiple assignment rules for a Task Queue, the rules are
evaluated in order, starting from index 0. The first applicable rule will be
applied and the rest will be ignored.

In the event that no assignment rule is applicable on a task (or the Task
Queue is simply not versioned), the tasks will be dispatched to an
unversioned Worker.

### Redirect Rules
Redirect rules should only be used when you want to move workflows and activities assigned to
one Build ID (source) to another compatible Build ID (target). You are responsible to make sure
the target Build ID of a redirect rule is able to process event histories made by the source
Build ID by using [Patching](https://docs.temporal.io/workflows#patching) or other means.

Most deployments are not expected to need these rules, however following
 situations can greatly benefit from redirects:
  - Need to move long-running Workflow Executions from an old Build ID to a
    newer one.
  - Need to hotfix some broken or stuck Workflow Executions.

In steady state, redirect rules are beneficial when dealing with old
Executions ran on now-decommissioned Build IDs:
  - To redirecting the Workflow Queries to the current (compatible) Build ID.
  - To be able to Reset an old Execution so it can run on the current
    (compatible) Build ID.

Redirect rules can be chained.

## How to Use Worker Versioning
To use Worker Versioning, you need to:
1. Using Worker options to opt in Worker Versioning and pass Build ID. This is supported 
in all SDKs. 

Below is an example written in Go. It is assumed that the Build ID is provided via a env 
variable.
```go
workerOptions := worker.Options{
   BuildID: os.Getenv("BUILD_ID"),
   UseBuildIDForVersioning: true,
// ...
}
w := worker.New(c, "your_task_queue_name", workerOptions)
```
2. Deploy your worker with new Build ID while keeping the old Build ID still running.
2. [Use CLI to update Worker Versioning Rules](#update-rules-using-cli) 
to send new (and possibly existing, if you are deploying a compatible build) executions 
to the new Build ID. Depending on your deployment strategy you may need to update the 
rules multiple times.
3. Use CLI to get reachability status of the old build ID. Once the Build ID is not 
reachable, decommission the old Build ID.

**Note for Temporal Server Operators:** Worker Versioning needs to be enabled from server
before being able to run versioned workers or update the rules. To enable Worker Versioning
set the following dynamic configs to `true`: `frontend.workerVersioningRuleAPIs`, 
`frontend.workerVersioningWorkflowAPIs`. Both configs allow configuration globally,
per Namespace, or per Task Queue.


### Update Rules Using CLI
You can use `temporal task-queue versioning` commands to update and read the Versioning 
rules of a given Task Queue. Here are a few examples:

**Add Assignment rule:** send 10% of new executions to Build ID "abc-123".
```shell
temporal task-queue versioning insert-assignment-rule --task-queue MY_TQ --build-id abc-123 --percentage 10
```

**Commit Build ID** to complete the rollout of "abc-123" and cleanup unnecessary rules.
```shell
temporal task-queue versioning commit-build-id --task-queue MY_TQ --build-id abc-123
```

**Add Redirect rule** from "abc-123" to "xyz-789":
```shell
temporal task-queue versioning add-redirect-rule --task-queue MY_TQ --source-build-id abc-123 --target-build-id xyz-789
```

**List Versioning rules**:
```shell
temporal task-queue versioning get-rules --task-queue MY_TQ
```

### Get Reachability Info via CLI
You can use `temporal task-queue describe` command to get reachability status of a Build ID.
Example: report reachability of Build ID "abc-123" on Task Queue MY_TQ:
```shell
temporal task-queue describe --task-queue MY_TQ --select-build-id abc-123 --report-reachability
```

The reachability status can be one of the following:
- **REACHABLE:** Build ID may be used by new workflows or activities (base on versioning 
rules), or there MAY be open workflows or backlogged activities assigned to it.
- **CLOSED_WORKFLOWS_ONLY:** Build ID does not have open workflows and is not reachable
by new workflows, but MAY have closed workflows within the namespace retention period. 
Not applicable to activity-only task queues.
- **UNREACHABLE:** Build ID is not used for new executions, nor it has been used by 
any existing execution within the retention period.
- **UNSPECIFIED:** Task reachability is not reported

#### Caveats:
- Task Reachability is eventually consistent; there may be a delay until it converges to the most
accurate value but it is designed in a way to take the more conservative side until it converges.
For example REACHABLE is more conservative than CLOSED_WORKFLOWS_ONLY.

- Future activities who inherit their workflow's Build ID but not its Task Queue will not be
accounted for reachability as server cannot know if they'll happen as they do not use
assignment rules of their Task Queue. Same goes for Child Workflows or Continue-As-New Workflows
who inherit the parent/previous workflow's Build ID but not its Task Queue. In those cases, make
sure to query reachability for the parent/previous workflow's Task Queue as well.

## Best Practices
[WIP]

## Planned Improvements
Worker Versioning is currently in Pre-Release (alpha) stage. We continue to improve this
feature before the public release. Here are some improvements planned for the coming months:

- [TBD]

## Migration Notes
### Migrating from Unversioned Task Queue
To migrate your existing Task Queue, follow the normal procedure as if you upgrade from
one Build ID to another.

The only limitation is that, as of now, redirect rules with unversioned source is not 
supported. Hence, if you want to redirect your long-running unversioned Workflows to a 
Build ID that is not possible. (This may change in the future.)

### Migrating from Version Sets
If you are using old Versioning API (i.e. using [Version Sets](https://docs.temporal.io/workers#defining-the-version-sets))
you can easily migrate to the new API.

You don't need to change your worker code or Task Queue name. Only for your next 
deployment, add an Assignment rule instead of a Version Set (incompatible Build ID).

The Version Sets added previously will be present and be used for the old Workflow
executions. They will be cleaned up automatically once all their workflows are archived.

Note that:
- For routing new executions, Assignment rules take precedence over the default Version Set.
- A single Build ID can either be added to a Version Set or Versioning Rules, not both.
- It's not possible to add a Redirect Rule from a Version Set (or a Build of a Version Set)
to another Build ID. This means you cannot redirect Workflows already started on a Version 
Set to a Build ID not in that Version Set.
- For Task Queues that have both Version Sets and Versioning Rules, Temporal UI only shows
Versioning Rules. The Version Sets are still retrievable using the following CLI command:
`temporal task-queue get-build-ids`
Worker Versioning
=================

# !!! This document is Deprecated !!!
_Please join the #safe-deploys channel in the community slack for further information._

---------

**Note 1:** In this iteration of Worker Versioning we deprecated the Version Set concept 
and APIs related to it. If you are using old Worker Versioning APIs please migrate
to the new APIs using the process [outlined in a later section](#migrating-from-version-sets).

**Note 2:** Worker Versioning is still in [Pre-Release](https://docs.temporal.io/evaluate/release-stages#pre-release) 
stage and not recommended for production usage. Future breaking changes may be made if deemed necessary.
We love feedback!  Please reach out in the Temporal Slack or at community.temporal.io with any thoughts.

## What is Worker Versioning?
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

## When should I use Worker Versioning?

### Short-running workflows
Worker versioning is currently optimized for short-running workflows.

The main reason to use this feature is that it frees you from having to worry about
nondeterministic changes. This is a significant gain, however it comes at the cost of
running multiple versions of Workers simultaneously.

For this reason, Worker Versioning is best suited for workers that run only short-lived 
workflows. Because the workers with the old Build ID are only needed to be kept for a 
short window during/after a deployment until all open workflows belonging to the old Build 
ID close.

For long-running Workflows, you can still use Worker Versioning using one of the following
approaches:
- Use versioning as you would for short-lived workflows, if you can afford running and 
managing multiple versions of your Workers for an extended duration. This might be
a suitable solutions if the expected deployment frequency during the average lifetime is
not high.
- OR, break long-running Workflows to shorter ones using Continue-as-New and set
`UseAssignmentRules` [VersioningIntent](https://pkg.go.dev/go.temporal.io/sdk@v1.27.0/internal#VersioningIntent) 
so each new CaN execution starts on the latest Build ID.
- OR, avoid making any nondeterministic changes by using [Patching](https://docs.temporal.io/workflows#patching) or [GetCurrentBuildID](https://pkg.go.dev/go.temporal.io/sdk@v1.27.0/internal#WorkflowInfo.GetCurrentBuildID)
  and consistently applying [Redirect rules](#redirect-rules) each time you deploy for
  those long-running workflows.

### Blue-green deploys
When juggling multiple Worker versions, you need each version to be able to handle the load 
placed upon it.  This can be difficult to calculate.  Therefore, the safest option is to operate 
each version at the normal capacity during your deployment.  
For this reason, we recommend a blue-green deploy strategy.

## Getting Started

### Prerequisites
Temporal Server version [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0) or higher is needed to use the new Worker Versioning API.
In addition, you need to use Temporal CLI or Go SDK for updating versioning rules (typically needed only when you do a new deployment).
You can use the SDK of your choice, as listed below, to create and run workers that can opt into Worker Versioning.


- Updating and getting versioning rules is supported by:
  - CLI >= [v0.13.1](https://github.com/temporalio/cli/releases/tag/v0.13.1)
  - Go SDK >= [v1.27.0](https://github.com/temporalio/sdk-go/releases/tag/v1.27.0)
- Creating and running a versioned worker is supported by the following SDKs:
  - Go SDK >= [v1.23.0](https://github.com/temporalio/sdk-go/releases/tag/v1.23.0)
  - Java SDK >= [v1.20.0](https://github.com/temporalio/sdk-java/releases/tag/v1.20.0)
  - TypeScript SDK >= [v1.8.0](https://github.com/temporalio/sdk-typescript/releases/tag/v1.8.0)
  - Python SDK >= [v1.3.0](https://github.com/temporalio/sdk-python/releases/tag/1.3.0)
  - .NET SDK >= [v0.1.0-beta1](https://github.com/temporalio/sdk-dotnet/releases/tag/0.1.0-beta1)
- Listing versioning rules in the UI is supported by:
  - UI >= [v2.28.0](https://github.com/temporalio/ui-server/releases/tag/v2.28.0)

In order to run versioned workers and update the versioning rules for each task queue,
Worker Versioning needs to be enabled in the server.

To start a dev cluster with versioning enabled:
```shell
temporal server start-dev \
   --dynamic-config-value frontend.workerVersioningWorkflowAPIs=true \
   --dynamic-config-value frontend.workerVersioningRuleAPIs=true
```

To enable Worker Versioning in a self-hosted server, the following dynamic config fields must be set to `true`.
They can be set globally, per Namespace, or per Task Queue:
- `frontend.workerVersioningRuleAPIs`
- `frontend.workerVersioningWorkflowAPIs`

In Temporal Cloud, open a ticket against the support team to enable the feature and be included in [pre-release](https://docs.temporal.io/evaluate/release-stages#pre-release).

### 0. Existing load
This walkthrough assumes that there is some constant load regularly starting short-running workflow executions
and an existing worker processing them. The workflow/worker could be versioned or unversioned.

### 1. Define your new versioned worker
In the worker options, pass in the Build ID your worker will poll on, _and_ opt in to use the
Build ID for versioning. The best practice is to provide a new Build ID generated by your build
pipeline whenever a new build of your code is made. Below is an example written in Go.
```go
w := worker.New(
        c, "my-tq", worker.Options{
            UseBuildIDForVersioning: true,
            BuildID:                 os.Getenv("BUILD_ID"),
        },
)
```

### 2. Build and deploy your new versioned worker
Build and deploy your worker with the tool of your choice (i.e. docker). The worker will begin
polling for tasks scheduled to the Build ID you specified, but it won't receive any tasks until
the versioning rules are updated.

### 4. Assign 1% of new workflow executions to your new Build ID
[Add an assignment rule](#update-rules-using-cli) targeting your Build ID to send 1% of new
tasks to your versioned worker.
```shell
temporal task-queue versioning insert-assignment-rule --task-queue my-tq --build-id $BUILD_ID --percentage 1
```
Once this rule is created, 1% of new workflow executions will be sent to the versioned
worker that you started above. The remaining 99% of new workflow executions will be assigned to the default
Build ID, which is the first Build ID in the assignment rule list that has a ramp of 100%, or unversioned
if no such rule exists.

### 5. Monitor workflows running on the new version and confirm success
You can check the progress of workflows filtered by Build ID like this:
```shell
temporal workflow list --query "BuildIds = 'assigned:$BUILD_ID'"
```

### 6. Commit the Build ID to make it the new default version
This will atomically delete the partially-ramped rule you added in step 4 and replace it with
a rule with 100% ramp.
```shell
temporal task-queue versioning commit-build-id --task-queue my-tq --build-id $BUILD_ID
```
See the `--help` output for more details:
```shell
% temporal task-queue versioning commit-build-id --help
```
Completes the rollout of a BuildID and cleans up unnecessary rules possibly
created during a gradual rollout. Specifically, this command will make the
following changes atomically:
	1. Adds an unconditional assignment rule for the target Build ID at the end of the list.
	2. Removes all previously added assignment rules to the given target Build ID.
	3. Removes any unconditional assignment rules for other Build IDs.

To prevent committing invalid Build IDs, we reject the request if no pollers
have been seen recently for this Build ID. Use the force option to disable this validation.

### 7. Wait until the old workers are not needed
Check the reachability of your old version. Now that all new workflow executions are assigned to the new Build ID,
your old worker (that was running in step 0) will not receive any tasks from new workflow executions. Running workflow
executions will send their outstanding tasks to workers with the version that the workflow execution is assigned to 
(barring other instructions via [Redirect Rules](#redirect-rules)).  Unless your application performs queries to closed workflows, a worker is no longer needed after the reachability status transitions from `REACHABLE` to `CLOSED_WORKFLOWS_ONLY`.

```shell
temporal task-queue describe --task-queue my-tq --select-build-id $OLD_BUILD_ID --report-reachability
```

Note: If your previous default was unversioned, replace `--select-build-id $OLD_BUILD_ID` with `--select-unversioned` in the above command.
If you omit the `--select-*` flags, results for the current default Build ID will be returned.


### 8. Decommission the previous versioned worker
After the reachability status for your old version and task queue is `CLOSED_WORKFLOWS_ONLY` you can safely 
decommission your old worker(s). See [Build ID Reachability](#build-id-reachability) for more details.


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

## Concepts

Temporal Server allows you to manage routing of tasks to Build IDs via 
**Worker Versioning Rules**.

Worker Versioning rules and added to a given Task Queue. If your Worker Program contains 
multiple Workers polling multiple Task Queues, you would need to separately update the rules 
of each Task Queue.

There are two types of rules: _Build ID Assignment rules_ and _Build ID Redirect rules_.

### Assignment Rules
Assignment rules are used to assign a Build ID for a new execution when it starts. Their primary
use case is to specify the latest Build ID, but they have powerful features for gradual rollout
of a new Build ID.

Once a Workflow Execution is assigned to a build ID, and it completes its first Workflow Task,
the workflow stays on that Build ID regardless of changes in Assignment rules. This
eliminates the need for compatibility between versions when you only care about using the new
version for new Workflows and let existing Workflows finish in their own version.

Activities, Child Workflows and Continue-as-New executions have the option to inherit the 
Build ID of their parent/previous Workflow or use the latest Assignment rules to independently 
select a Build ID. This is specified by the parent/previous Workflow using VersioningIntent.
We recommend that you allow Continued-as-New workflows to be assigned new versions, otherwise your
workflow may become like a long-running workflow that gets stuck on the old build.

Unless there's a redirect rule for it, the task will be dispatched to Workers of the Build ID determined by the Assignment rules (or inherited).

When using Worker Versioning on a Task Queue, in the steady state,
there should typically be a single assignment rule to send all new executions
to the latest Build ID. Existence of at least one such "unconditional"
rule at all times is enforced by the system, unless the `force` flag is used
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

Here are situations you might need a redirect rule: 
  - Need to move long-running Workflow Executions to a newer build ID.
  - You have stuck Workflow Executions that you want to move to a later version, to fix them or
    so that you can decommission the old version.
  - Workflow queries will not run if sent to a closed workflow that ran on a decommissioned build.
    You would need redirect rules to allow those queries to run on newer builds.

Redirect rules can be chained.

### Build ID Reachability
Temporal Server can help you decide when to safely decommission workers of an old Build ID
by providing *Task Reachability* status for that Build ID.

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
- It's safe to rely on Reachability status for shutting down old workers.  However, it may take a bit 
longer to converge due to possible delays, usually no more than a few minutes, in the status being updated. 

- Future activities who inherit their workflow's Build ID but not its Task Queue will not be
accounted for reachability as server cannot know if they'll happen as they do not use
assignment rules of their Task Queue. Same goes for Child Workflows or Continue-As-New Workflows
who inherit the parent/previous workflow's Build ID but not its Task Queue. In those cases, make
sure to query reachability for the parent/previous workflow's Task Queue as well.

## Planned Improvements
Worker Versioning is currently in [Pre-Release](https://docs.temporal.io/evaluate/release-stages#pre-release). 
We will continue to improve this feature before public preview. 
We plan to respond to your feedback, so please reach out!
In addition, here are likely improvements and behavior changes being considered for public preview:

- Allow workflow queries on closed workflows to run on newer builds.
- Ways to more gradually redirect workflows 
- Ability to apply redirect rules from unversioned task queues.  This may help users first onboarding
  to worker versioning
- Default continued-as-new workflows to run on later builds rather than pegging them to the build
  of the previous workflow.
- Docs, API improvements, polish.
- More integration with Temporal UI.
- Improvements for multi-region namespaces.

## Migration Notes
### Migrating from an Unversioned Task Queue
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
The assignment rule can generally be done using the `insert-assignment-rule` command, 
however, `commit-build-id` provides an idempotent replacement to both (now-deprecated) 
`promote-set` and `add-new-default` operations. 

The Version Sets added previously will be present and be used for the old Workflow
executions. They will be cleaned up automatically once all their workflows pass their retention time.

Note that:
- For routing new executions, Assignment rules take precedence over the default Version Set.
- If deploying your first build ID using the new API fails and you want to roll back,
remove all the assignment rules so that the default Version Sets previously added be used
again.
- A single Build ID can either be added to a Version Set or Versioning Rules, not both.
- It's not possible to add a Redirect Rule from a Version Set (or a Build of a Version Set)
to another Build ID. This means you cannot redirect Workflows already started on a Version 
Set to a Build ID not in that Version Set.
- For Task Queues that have both Version Sets and Versioning Rules, Temporal UI only shows
Versioning Rules. The Version Sets are still retrievable using the following CLI command:
`temporal task-queue get-build-ids`
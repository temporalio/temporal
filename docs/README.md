# Premises

The following are some fundamental premises of Temporal workflows:

**Requirements**

- Workflows are defined as code, in one of the supported [SDK languages](https://docs.temporal.io/dev-guide).

- _Durable execution_ of workflows must be guaranteed: workflows must still execute correctly in the face of transient failures in user-hosted processes.

- The system can be scaled to handle arbitrarily many concurrent workflow executions.

- User code is not transmitted outside user systems.

**Design decisions**

- The system functions via event sourcing: an append-only history of events is stored for each workflow execution, and all required workflow state can be recreated at any time by replaying this history.

- User code defining workflows is segregated into [Workflow](https://docs.temporal.io/workflows) definitions and [Activity](https://docs.temporal.io/activities) definitions. Workflow code is highly constrained: side-effects are permitted only in Activity code.

# High-level architecture

These premises have led to a system architecture which is divided into user-hosted processes, versus the Temporal server processes (which may be hosted outside the user's systems):

<!-- https://lucid.app/lucidchart/0202e4b8-5258-4cd6-a6a0-67159300532b/edit -->
<img src="https://github.com/temporalio/temporal/assets/52205/761ed417-98d9-4b37-913f-f1d23223bb2f">

#### User-hosted processes

- The user's application uses one of the [Temporal SDKs](https://docs.temporal.io/dev-guide) to communicate with the Temporal server to start/cancel workflows, and interact with running workflows.

- In addition, the user segregates some of their application code into Temporal Workflow and Activity definitions, and hosts [Worker](https://docs.temporal.io/workers) processes, which execute their Workflow and Activity code. Workflow and Activity code uses the SDK as a library, and the Worker runtime is implemented by the SDK.

- The worker processes communicate with the Temporal server in two ways: they continuously poll the server for tasks, and on completion of each task they send commands to the server specifying what must be done to further advance the workflow execution. See [Tasks](./#Tasks) below.

#### Temporal Server processes

- History service shards manage individual [Workflow Executions](https://docs.temporal.io/workflows#workflow-execution). They handle RPCs originating from the User Application and the Temporal Worker, drive the Workflow Execution to completion by enqueuing Workflow and Activity Tasks in the Matching Service, and store all state required for durable execution of the workflow.
- Matching service shards manage the [Task Queues](https://docs.temporal.io/workers#task-queue) being polled by Temporal Worker processes. A single task queue holds tasks for multiple Workflow Executions.
- Users can host and operate the Temporal server and its database themselves, or use [Temporal Cloud](https://temporal.io/cloud).

## Tasks

Temporal Workers poll the Task Queues in the Matching service for tasks. There are two types of task:

- A **Workflow Task** is processed by resuming execution of the user's workflow code until it becomes blocked (e.g. on a timer or an Activity call), or is complete. On completion of a Workflow Task the worker sends a sequence of commands specifying what is required to advance the workflow (e.g. set a timer, schedule an Activity task).

- An **Activity Task** is processed by attempting to execute an Activity. On completion of an Activity Task (whether success or failure), the worker sends information about the activity outcome to the server.

# Data storage

The Go application performs all data access operations via interfaces and thus is agnostic about the details of the underlying data storage. In practice however, implementations exist for Cassandra, MySQL, Postgres, and SQLite (see [`schema/`](https://github.com/temporalio/temporal/blob/ef49189005b5323c532264287af6c08a447aab8a/schema) directory).

# Additional documentation

- [Workflow lifecycle](./workflow-lifecycle.md)

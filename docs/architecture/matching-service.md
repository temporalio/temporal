# Matching Service

[see [API definition](https://github.com/temporalio/temporal/blob/main/proto/internal/temporal/server/api/matchingservice/v1/service.proto)]

<!-- https://lucid.app/lucidchart/0202e4b8-5258-4cd6-a6a0-67159300532b/edit -->
<img src="../_assets/matching-context.svg">

## Task Queues
Matching Service instances manage [Task Queues](https://docs.temporal.io/workers#task-queue) being polled by Temporal Worker processes.
Long-poll requests from Temporal Workers are received by the Frontend Service, which routes them to the Matching Service instance responsible for the requested Task Queue.
The Matching Service instance responds by sending Workflow Tasks and Activity Tasks from the requested Task Queue.
A single Task Queue is responsible for delivering tasks relating to many Workflow Executions.

### Task Queue Partitions
Matching Service splits Task Queues into partitions to provide higher throughput overall. Default is 4, but there can be less or more. Partition ownership can be reassigned, and partitions' metadata and task backlog can be loaded/unloaded from storage.

When throughput of tasks is low or polling by workers is rare, a polling worker can be "forwarded" from an empty partition to its parent partition. This is also true for a task on a partition which is not being polled, the task can be "forwarded" to a parent partition, hoping to find a poller. For small numbers of partitions, the root partition is the direct parent of all children, but with more partitions, the parent relationship forms a tree of depth > 2, converging at the root partition. If a root partition of a Task Queue is loaded, this will force all other partitions of that Task Queue to also load. This ensures that forwarding can occur between a child partition with a task in its backlog and a long-awaited poller.

Additional Documentation of Matching Service internals is not yet available.

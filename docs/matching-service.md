# Matching Service

_[see [API definition](https://github.com/temporalio/temporal/blob/main/proto/internal/temporal/server/api/matchingservice/v1/service.proto)]_

The Matching Service instances manage [Task Queues](https://docs.temporal.io/workers#task-queue).
The Temporal Worker processes long-poll for tasks.

<img src="./diagrams/matching-context.svg">

The Matching Service instance responds by sending Workflow Tasks and Activity Tasks from the requested Task Queue.
A single Task Queue is responsible for delivering tasks relating to many Workflow Executions.

The Matching Service is responsible for _matching_ Workers to Tasks and routing new Tasks to the 
appropriate queue.

Long-poll requests from Temporal Workers are received by the Frontend Service, which routes them to the Matching Service instance responsible for the requested Task Queue.

When tasks (we will say "tasks" here to mean any of query tasks, workflow tasks, and activity tasks as they are not meaningfully different in the context of matching) are deemed runnable by the Temporal history service, they are pushed to the matching service, whose job it is to dispatch those tasks to workers for execution. For their part, workers poll the matching service for runnable tasks. Task production and consumption is always targeted at a named task queue and thus we can think of the matching service as the host of some number of named rendezvous points for task exchange between producers and consumers.

The matching service and Temporal in general uses the term "queue" or "taskqueue" to refer to named task exchange points and so that nomenclature is adopted here, but tasks are not exchanged in FIFO order. This is not a problem - the Temporal system does not need FIFO task scheduling - but it may be surprising.

<img src="./diagrams/matching-components.svg">

<img src="./diagrams/matching-deployment.svg">

<img src="./diagrams/matching-partitions.svg">
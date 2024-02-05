# Matching Service

_[see [API](https://github.com/temporalio/temporal/blob/main/proto/internal/temporal/server/api/matchingservice/v1/service.proto)]_

The Matching Service instances manage [Task Queues](https://docs.temporal.io/workers#task-queue).
The Temporal Worker processes long-poll for tasks.


The Matching Service instance responds by sending Workflow Tasks and Activity Tasks from the requested Task Queue.
A single Task Queue is responsible for delivering tasks relating to many Workflow Executions.

The Matching Service is responsible for _matching_ Workers to Tasks and routing new Tasks to the 
appropriate queue.

## Design

### Context

Long-poll requests from Temporal Workers
are received by the Frontend Service, which routes them to the Matching Service instance responsible for the requested Task Queue.

### Long-poll

### Partitions

## Implementation

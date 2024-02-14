# Matching Service

_[see [API definition](https://github.com/temporalio/temporal/blob/main/proto/internal/temporal/server/api/matchingservice/v1/service.proto)]_

<img src="../../docs/assets/matching-context.svg">

Matching Service instances manage Task Queues being polled by Temporal Worker processes. Long-poll requests from Temporal Workers are received by the Frontend Service, which routes them to the Matching Service instance responsible for the requested Task Queue. The Matching Service instance responds by sending Workflow Tasks and Activity Tasks from the requested Task Queue. A single Task Queue is responsible for delivering tasks relating to many Workflow Executions.

Documentation of Matching Service internals is not yet available.
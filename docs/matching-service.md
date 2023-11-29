# Matching Service

<!-- https://lucid.app/lucidchart/0202e4b8-5258-4cd6-a6a0-67159300532b/edit -->
<img src="https://github.com/temporalio/documentation/assets/52205/cf0529ee-23cf-464b-8efe-a4f1d1f17b37">

Matching Service instances manage [Task Queues](https://docs.temporal.io/workers#task-queue) being polled by Temporal Worker processes.
Long-poll requests from Temporal Workers are received by the Frontend Service, which routes them to the Matching Service instance responsible for the requested Task Queue.
The Matching Service instance responds by sending Workflow Tasks and Activity Tasks from the requested Task Queue.
A single Task Queue is responsible for delivering tasks relating to many Workflow Executions.

Documentation of Matching Service internals is not yet available.

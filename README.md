# Cadence

Cadence is a distributed, scalable, durable, and highly available orchestration engine we developed at Uber Engineering to execute asynchronous long-running business logic in a scalable and resilient way.

Business logic is modeled as workflows and activities. Workflows are the implementation of coordination logic. Its sole purpose is to orchestrate activity executions. Activities are the implementation of a particular task in the business logic. The workflow and activity implementation are hosted and executed in worker processes. These workers long-poll the Cadence server for tasks, execute the tasks by invoking either a workflow or activity implementation, and return the results of the task back to the Cadence server. Furthermore, the workers can be implemented as completely stateless services which in turn allows for unlimited horizontal scaling.

The Cadence server brokers and persists tasks and events generated during workflow execution, which provides certain scalability and realiability guarantees for workflow executions. An individual activity execution is not fault tolerant as it can fail for various reasons. But the workflow that defines in which order and how (location, input parameters, timeouts, etc.) activities are executed is guaranteed to continue execution under various failure conditions.

This repo contains the source code of the Cadence server. The client lib you can use to implement workflows, activities and worker can be found [here](https://github.com/uber-go/cadence-client).

See Maxim's talk at [Data@Scale Conference](https://atscaleconference.com/videos/cadence-microservice-architecture-beyond-requestreply) for an architectural overview of Cadence.

## Getting Started

### Start the cadence-server locally

* Build the required binaries following the instructions [here](CONTRIBUTING.md).

* Install and run `cassandra` locally:
```bash
# for OS X
brew install cassandra

# start cassandra
/usr/local/bin/cassandra
```

* Setup the cassandra schema:
```bash
./cadence-cassandra-tool --ep 127.0.0.1 create -k "cadence" --rf 1
./cadence-cassandra-tool --ep 127.0.0.1 -k "cadence" setup-schema -d -f ./schema/cadence/schema.cql
./cadence-cassandra-tool --ep 127.0.0.1 create -k "cadence_visibility" --rf 1
./cadence-cassandra-tool --ep 127.0.0.1 -k "cadence_visibility" setup-schema -d -f ./schema/visibility/schema.cql
```

* Start the service:
```bash
./cadence
```

### Using Docker

You can also [build and run](docker/README.md) the service using Docker.

### Run the Samples

Try out the sample recipes [here](https://github.com/samarabbas/cadence-samples) to get started.

## Contributing
We'd love your help in making Cadence great. Please review our [instructions](CONTRIBUTING.md).

## License

MIT License, please see [LICENSE](https://github.com/uber/cadence/blob/master/LICENSE) for details.

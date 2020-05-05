# Cadence Roadmap

The following is a high-level quarterly roadmap of the [Cadence](https://cadenceworkflow.io/) project.
Please contact [Cadence discussion group](https://groups.google.com/d/forum/cadence-discussion) for more information.

## Q3 2019

* [Resource-Specific Tasklist (a.k.a session)](https://github.com/uber/cadence/blob/master/docs/design/1533-host-specific-tasklist.md)
* [Visibility on Elastic Search](https://github.com/uber/cadence/blob/master/docs/visibility-on-elasticsearch.md)
* Scalable tasklist
* MySQL support

## Q4 2019

* [Multi-DC support for Cadence replication](https://github.com/uber/cadence/blob/master/docs/design/2290-cadence-ndc.md)
* Workflow history archival
* Workflow visibility archival
* [Synchronous Request Reply](https://github.com/uber/cadence/blob/master/docs/design/2215-synchronous-request-reply.md)
* Postgres SQL support

## Q1 2020

* Service availability and reliability improvements
* [Domain level AuthN and AuthZ support](https://github.com/uber/cadence/issues/2833)
* Graceful domain failover design
* UI bug fixes and performance improvements

## Q2 2020

* Kafka deprecation for Cadence replication
* Graceful domain failover
* Multi-tenancy task prioritization and resource isolation
* UI and CLI feature parity

## Q3 2020 and beyond

* GRPC support
* Parent-child affinity
* Task priority
* Reset 2.0 (more flexibility and handle child)
* Safe workflow code rollout
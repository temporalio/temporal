[![Build status](https://badge.buildkite.com/fc0e676d7bee1a159916af52ebdb541708d4b9f88b8a980f6b.svg)](https://buildkite.com/temporal/temporal-server)
[![Coverage Status](https://coveralls.io/repos/github/temporalio/temporal/badge.svg?branch=master)](https://coveralls.io/github/temporalio/temporal?branch=master)
[![Discourse](https://img.shields.io/static/v1?label=Discourse&message=Get%20Help&color=informational)](https://community.temporal.io)

# Temporal  

Temporal is a microservice orchestration platform which enables developers to build scalable applications without sacrificing productivity or reliability.
Temporal server executes units of application logic, Workflows, in a resilient manner that automatically handles intermittent failures, and retries failed operations.

The programming model of Temporal exposed via the SDKs (currently for Java and Go) offers developers the simple abstractions of Workflows and Activities that can virtually "resume" execution after a failure from the same line of code.

Temporal is a mature technology, a fork of Uber's Cadence.
Temporal is being developed by [Temporal Technologies](https://temporal.io/), a startup by the creators of Cadence.

[![IMAGE ALT TEXT](http://img.youtube.com/vi/f-18XztyN6c/0.jpg)](http://www.youtube.com/watch?v=f-18XztyN6c "Temporal")

Learn more about Temporal at [docs.temporal.io](https://docs.temporal.io).

## Getting Started

### Download and Start Temporal Server Locally

Execute the following commands to start a pre-built image along with all the dependencies.

```bash
$ curl -L https://github.com/temporalio/temporal/releases/latest/download/docker.tar.gz | tar -xz
$ cd docker
$ docker-compose up
```

Refer to [Temporal service docker](docker/README.md) for more advanced options.

### Run the Samples

Clone or download samples for [Go](https://github.com/temporalio/samples-go) or [Java](https://github.com/temporalio/samples-java) and try to run them against the local Temporal server.
We have a number of [HelloWorld type scenarios](https://github.com/temporalio/samples-java#helloworld) available.

### Use CLI

Use [Temporal's command line tool](https://docs.temporal.io/docs/tctl) `tctl` to interact with the local Temporal server.

```bash
$ tctl namespace list
$ tctl workflow list
```

### Use Temporal Web

Try [Temporal Web UI](https://github.com/temporalio/web) by opening [http://localhost:8088](http://localhost:8088)for viewing your sample workflows executing on Temporal.

## Repository

This repository contains the source code of the Temporal server. To implement Workflows, Activities and Workers, use [Go SDK](https://github.com/temporalio/sdk-go) or [Java SDK](https://github.com/temporalio/sdk-java).

## Contributing

We'd love your help in making Temporal great. Please review our [contribution guide](CONTRIBUTING.md).

If you'd like to work on or propose a new feature, first peruse [feature requests](https://community.temporal.io/c/feature-requests/6) and our [proposals repo](https://github.com/temporalio/proposals) to discover existing active and accepted proposals.

Feel free to join the Temporal [Slack channel](https://join.slack.com/t/temporalio/shared_invite/zt-c1e99p8g-beF7~ZZW2HP6gGStXD8Nuw) to start a discussion or check if a feature has already been discussed.
Once you're sure the proposal is not covered elsewhere, please follow our [proposal instructions](https://github.com/temporalio/proposals#creating-a-new-proposal) or submit a [feature request](https://community.temporal.io/c/feature-requests/6).

## License

[MIT License](https://github.com/temporalio/temporal/blob/master/LICENSE)

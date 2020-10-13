# Temporal  
[![Build status](https://badge.buildkite.com/fc0e676d7bee1a159916af52ebdb541708d4b9f88b8a980f6b.svg)](https://buildkite.com/temporal/temporal-server)
[![Coverage Status](https://coveralls.io/repos/github/temporalio/temporal/badge.svg?branch=master)](https://coveralls.io/github/temporalio/temporal?branch=master)
[![Discourse](https://img.shields.io/static/v1?label=Discourse&message=Get%20Help&color=informational)](https://community.temporal.io)

Visit [docs.temporal.io](https://docs.temporal.io) to learn about Temporal.

This repo contains the source code of the Temporal server. To implement workflows, activities and worker use [Go SDK](https://github.com/temporalio/sdk-go) or [Java SDK](https://github.com/temporalio/sdk-java).

See Maxim's talk at [Data@Scale Conference](https://atscaleconference.com/videos/cadence-microservice-architecture-beyond-requestreply) for an architectural overview of Temporal.

## Getting Started

### Start the temporal-server locally

We highly recommend that you use [Temporal service docker](docker/README.md) to run the service.

### Run the Samples

Try out the sample recipes for [Go](https://github.com/temporalio/samples-go) or [Java](https://github.com/temporalio/samples-java) to get started.

### Use CLI

Try out [Temporal command-line tool](tools/cli/README.md) to perform various tasks on Temporal

### Use Temporal Web

Try out [Temporal Web UI](https://github.com/temporalio/web) to view your workflows on Temporal.  
(This is already available at localhost:8088 if you run Temporal with docker compose)

## Contributing

We'd love your help in making Temporal great. Please review our [contribution guide](CONTRIBUTING.md).

If you'd like to work on or propose a new feature, first peruse [feature requests](https://community.temporal.io/c/feature-requests/6) and our [proposals repo](https://github.com/temporalio/proposals) to discover existing active and accepted proposals. Feel free to join the Temporal [Slack channel](https://join.slack.com/t/temporalio/shared_invite/zt-c1e99p8g-beF7~ZZW2HP6gGStXD8Nuw) to start a discussion or check if a feature has already been discussed. Once you're sure the proposal is not covered elsewhere, please follow our [proposal instructions](https://github.com/temporalio/proposals#creating-a-new-proposal) or submit a [feature request](https://community.temporal.io/c/feature-requests/6).

## License

MIT License, please see [LICENSE](https://github.com/temporalio/temporal/blob/master/LICENSE) for details.

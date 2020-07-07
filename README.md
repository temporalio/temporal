# Temporal  
[![Build status](https://badge.buildkite.com/fc0e676d7bee1a159916af52ebdb541708d4b9f88b8a980f6b.svg)](https://buildkite.com/temporal/temporal-server)
[![Coverage Status](https://coveralls.io/repos/github/temporalio/temporal/badge.svg?branch=master)](https://coveralls.io/github/temporalio/temporal?branch=master)
[![Discourse](https://img.shields.io/static/v1?label=Discourse&message=Get%20Help&color=informational)](https://community.temporal.io)

Visit [docs.temporal.io](https://docs.temporal.io) to learn about Temporal.

This repo contains the source code of the Temporal server. To implement workflows, activities and worker use [Go SDK](https://github.com/temporalio/temporal-go-client) or [Java SDK](https://github.com/temporalio/temporal-java-client).

See Maxim's talk at [Data@Scale Conference](https://atscaleconference.com/videos/cadence-microservice-architecture-beyond-requestreply) for an architectural overview of Temporal.

## Getting Started

### Start the temporal-server locally

We highly recommend that you use [Temporal service docker](docker/README.md) to run the service.

### Run the Samples

Try out the sample recipes for [Go](https://github.com/temporalio/temporal-go-samples) or [Java](https://github.com/temporalio/temporal-java-samples) to get started.

### Use CLI

Try out [Temporal command-line tool](tools/cli/README.md) to perform various tasks on Temporal

### Use Temporal Web

Try out [Temporal Web UI](https://github.com/temporalio/temporal-web) to view your workflows on Temporal.  
(This is already available at localhost:8088 if you run Temporal with docker compose)

## Contributing

We'd love your help in making Temporal great. Please review our [contribution guide](CONTRIBUTING.md).

If you'd like to propose a new feature, first join the Temporal [Slack channel](https://join.slack.com/t/temporalio/shared_invite/zt-c1e99p8g-beF7~ZZW2HP6gGStXD8Nuw) to start a discussion and check if there are existing design discussions. Also peruse our [design docs](docs/design/index.md) in case a feature has been designed but not yet implemented. Once you're sure the proposal is not covered elsewhere, please follow our [proposal instructions](PROPOSALS.md).

## License

MIT License, please see [LICENSE](https://github.com/temporalio/temporal/blob/master/LICENSE) for details.

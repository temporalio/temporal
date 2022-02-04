# Using Buildkite

Buildkite simply runs docker containers. So it is easy to perform the 
same build locally that BuildKite will do.

## Testing the build locally

### One-Time Setup
Create an empty directory at `/usr/bin/buildkite-agent`. This directory is mounted as a volume and must be
present for the container to start.

### Run Locally
To run tests locally use `docker-compose run` command in `develop/buildkite` directory:

```bash
cd develop/buildkite
docker-compose run <container_name> <command>
```

Get `<container_name>` and `<command>` form [pipeline.yml](pipeline.yml):
```yaml
  - label: ":golang: integration test with cassandra"
    agents:
      queue: "default"
      docker: "*"
    command: "make integration-test-coverage"    # command to run in container
    artifact_paths:
      - "build/coverage/*.out"
    retry:
      automatic:
        limit: 1
    plugins:
      - docker-compose#v3.1.0:
          run: integration-test-cassandra        # container name
          config: docker-compose.yml
```

For example to run unit tests:
```bash
docker-compose run unit-test make unit-test-coverage
```
or run integration tests with Cassandra:
```bash
docker-compose run integration-test-cassandra make integration-test-coverage
```
or run integration tests with MySQL:
```bash
docker-compose run integration-test-mysql make integration-test-coverage
```

## Testing the build in Buildkite

Creating a PR against the master branch will trigger the Buildkite
build. Members of the Temporal team can view the build pipeline here:
[https://buildkite.com/temporal/temporal-server](https://buildkite.com/temporal/temporal-server).

Eventually this pipeline should be made public. It will need to ignore 
third party PRs for safety reasons.

# Using Buildkite

Buildkite simply runs docker containers. So it is easy to perform the 
same build locally that BuildKite will do.

## Testing the build locally
To run tests locally run the following commands in `.buildkite` directory.

Run unit tests:
```bash
$ docker-compose run unit-test make cover_profile
```

Run integration tests with Cassandra:
```bash
$ docker-compose run integration-test-cassandra make cover_integration_profile
```

Run integration tests with MySQL:
```bash
$ docker-compose run integration-test-mysql make cover_integration_profile
```

Note that Buildkite will run basically the same commands:
```yaml
  - label: ":golang: integration test with cassandra"
    agents:
      queue: "default"
      docker: "*"
    command: "make cover_integration_profile" # command to run in container
    artifact_paths:
      - "build/coverage/*.out"
    retry:
      automatic:
        limit: 1
    plugins:
      - docker-compose#v3.1.0:
          run: integration-test-cassandra # container name
          config: docker-compose.yml
```

## Testing the build in Buildkite
Creating a PR against the master branch will trigger the Buildkite
build. Members of the Temporal team can view the build pipeline here:
[https://buildkite.com/temporal/temporal-server](https://buildkite.com/temporal/temporal-server).

Eventually this pipeline should be made public. It will need to ignore 
third party PRs for safety reasons.

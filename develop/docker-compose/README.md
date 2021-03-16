# Temporal Server docker-compose files for server development

These docker-compose files run Temporal server development dependencies. Basically, they run everything you need to run
Temporal server besides a server itself which you suppose to run locally on the host in your favorite IDE or as binary.   

You are not supposed to use these files directly. Please use [Makefile](../../Makefile) targets instead. To start dependencies:

```bash
make start-dependencies
```

To stop dependencies:

```bash
make stop-dependencies
```

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

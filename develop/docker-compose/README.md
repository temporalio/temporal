# Temporal Server `docker compose` files for server development

These `docker compose` files run Temporal server development dependencies. Basically, they run everything you need to run
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

## MongoDB replica set support

- The shared docker-compose definitions now start a single-node MongoDB replica set on `mongodb://temporal:temporal@localhost:27017`.
- Transactions are enabled via `rs0`; the replica set is initialized by `mongodb-init/init-replica.js` during container startup.
- Use the `temporal` user (password `temporal`) when pointing Temporal Server at the local MongoDB instance. For the official Mongo image this user is created in the `admin` database, so clients typically need `authSource=admin`.

To run the shared persistence integration suites against this MongoDB instance (see `common/persistence/tests`), configure:

- `MONGODB_SEEDS` (default: `127.0.0.1`)
- `MONGODB_PORT` (default: `27017`)
- `MONGODB_REPLICA_SET` (default: `rs0`)

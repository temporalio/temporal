# MongoDB Temporal Schema v1.0

The `init.js` helper provisions the collections required by the in-progress MongoDB persistence plugin. It is intentionally conservative:

- Creates collections if they do not already exist.
- Ensures the indexes currently defined in the Go implementation exist.
- Does **not** drop or recreate collections/indexes.

As more features land (ExecutionStore CRUD, QueueV2, Nexus), extend this version or introduce a new version folder with the necessary changes.

## Running

```
MONGO_URI="mongodb://localhost:27017" \
TEMPORAL_MONGO_DB="temporal" \
mongosh "$MONGO_URI" schema/mongodb/temporal/versioned/v1.0/init.js
```

Environment variables:

- `TEMPORAL_MONGO_DB` (optional) defaults to `temporal`.
- `TEMPORAL_MONGO_VISIBILITY_DB` (optional) can override the database used for the visibility store collections.

> The script logs no output when collections already exist. Use `--verbose` (mongosh flag) or run `db.getCollectionInfos()` to verify results.

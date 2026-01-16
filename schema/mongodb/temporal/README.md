# Temporal MongoDB Schema

This directory contains versioned MongoDB DDL artifacts for the Temporal persistence plugin.

## Layout

```
schema/mongodb/temporal/
├── README.md              # This file
└── versioned/
    └── v1.0/              # Initial schema definition (subject to updates)
        ├── README.md
        └── init.js        # Helper script creating collections and baseline indexes
```

## Conventions

- Scripts are written for `mongosh`. They can be executed locally or as part of a deployment pipeline.
- Each version directory should contain idempotent helpers. Future migrations can be added as additional scripts within the same version folder or as new version folders.
- Collections and indexes defined here mirror the names used in `common/persistence/mongodb/*`.

## Applying The Schema

```
# Example: apply v1.0 baseline
env MONGO_URI="mongodb://localhost:27017" \
env TEMPORAL_MONGO_DB="temporal" \
mongosh "$MONGO_URI" schema/mongodb/temporal/versioned/v1.0/init.js
```

> **Note**
> The scripts intentionally do not drop or mutate existing collections. They create collections and indexes when missing so they are safe to rerun.

## Next Steps

- Extend `init.js` as new collections or indexes are introduced.
- Add migration notes under new version directories when backward-incompatible changes are required.
- Coordinate schema updates with documentation updates under `docs/development/persistence/mongodb-persistence-plugin.md`.

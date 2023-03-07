# Temporalite

> ⚠️ This package is currently experimental and not suitable for production use. ⚠️

Temporalite is a distribution of Temporal that runs as a single process with zero runtime dependencies.

Persistence to disk and an in-memory mode are both supported via SQLite.

## Why

The goal of Temporalite is to make it simple and fast to run single-node Temporal servers for development, testing, and production use cases.

Features that align with this goal:

- High level library for configuring a SQLite backed Temporal server
- Fast startup time
- Minimal resource overhead: no dependencies on a container runtime or database server
- Support for Windows, Linux, and macOS

## Backwards Compatability

This package must not break backwards compatability in accordance with semantic versioning.

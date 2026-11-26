# Failpoints

Failpoints let tests and tools make server code fail, take alternate branches, or return values
from outside that code. The server provides a `*failpoint.Registry` only when enabled at startup.

## Enabling failpoints

Failpoints are compiled into every build, including release binaries; there is no separate test
image. A server process only installs them if the static server config (`config.Failpoints`) sets
`failpoints.enabled: true`, which it reads once at startup:

```yaml
failpoints:
  enabled: true
```

Neither environment variables nor dynamic config can enable failpoints. With the switch off, nothing
is installed: there is no failpoint registry, and call sites in server code cost a nil check. The
`test_dep` tag is not involved.

When the switch is on, the server logs a warning at startup that failpoints are enabled and not
meant for production use. With the switch off, it logs nothing about failpoints.

Test clusters (`testcore`) run the normal server with failpoints enabled. They give each server a
registry with `temporal.WithFailpointSource` instead of the switch, so that each cluster of a
test process has its own.

The server does not serve a gRPC API to configure failpoints. Tests configure the failpoints of
their cluster in process.

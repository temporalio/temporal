# Automatic Go memory limit

Temporal can set the process-wide Go memory limit to a fraction of the container's
memory limit. This is disabled by default. To enable it, add this block to the
server configuration:

```yaml
global:
  goMemoryLimit:
    ratio: 0.8
    refreshInterval: 1m
```

`ratio` accepts values from `0` to `1`. Zero disables automatic limiting.
`refreshInterval` defaults to zero, which applies the limit only at server start.
A positive interval rereads the container limit periodically. It does not reload
the ratio or interval from configuration.

The embedded configuration template accepts `TEMPORAL_GO_MEMORY_LIMIT_RATIO`
(default `0`) and `TEMPORAL_GO_MEMORY_LIMIT_REFRESH_INTERVAL` (default `0s`). These
variables do not override a plain YAML file or programmatically supplied config.
Custom configuration templates can read them using the existing `env` function.

## Overrides and failures

When the configured ratio is positive, the
[automemlimit library](https://github.com/KimMachineGun/automemlimit) applies these
overrides in order:

1. If `GOMEMLIMIT` is present, even if empty, leave the current Go limit unchanged
   and do not start refresh.
2. `AUTOMEMLIMIT=off` also disables automatic limiting and refresh.
3. Otherwise, a valid `AUTOMEMLIMIT` value overrides the configured ratio.

The configured ratio of zero disables the entire feature, regardless of
`AUTOMEMLIMIT`. `AUTOMEMLIMIT=0` is invalid; use `off` instead. Invalid configuration
values prevent startup. Environment variables are not reread during refresh.

Failure to read the container limit logs a warning and leaves the current Go
limit unchanged. Startup continues, and refresh retries if enabled. An explicitly
unlimited container is different: it removes the automatic Go cap, including
when a previously finite container limit is removed. Host memory is not used as
a fallback.

## Scope

The limit applies to the entire Go process, including all Temporal services in
that process. It is initialized before services start. Shutdown cancels refresh
after the services stop, but does not restore an earlier Go limit. Cancellation
does not interrupt a container-limit read already in progress.

Applications embedding multiple Temporal servers in one process should enable
this setting on only one server. They do not have separate Go memory budgets.
The separate UI server and tools that do not start a Temporal server are not
affected.

This is a [soft Go runtime limit](https://go.dev/doc/gc-guide#Memory_limit), not a
container limit or an OOM guarantee. Leave room for memory outside the Go runtime.
Setting it too low can increase garbage-collection CPU use, and a live working
set larger than the available memory can still cause an OOM.

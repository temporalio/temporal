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
is installed: there is no failpoint registry, BUGGIFY does nothing and buggifiable settings are not
randomized, the `failpoints.buggify` block is not read, and call sites in server code cost a nil
check. The `test_dep` tag is not involved.

When the switch is on, the server logs a warning at startup that failpoints are enabled and not
meant for production use. With the switch off, it logs nothing about failpoints.

Test clusters (`testcore`) run the normal server with failpoints enabled. They give each server a
registry with `temporal.WithFailpointSource` instead of the switch, so that each cluster of a
test process has its own.

The server does not serve a gRPC API to configure failpoints. Tests configure the failpoints of
their cluster in process.

## BUGGIFY

BUGGIFY runs a server "mostly normal, with a few weird things" by installing a seeded random
`failpoint.Policy` on the failpoint registry. With a seed, each failpoint key name that has nothing
registered is activated for the whole run with some probability, and each execution of an activated
key fires with another probability. Both decisions are pure functions of the seed, the key name and
the number of the execution of the key in its scope, so the same seed activates the same keys in
every namespace, and one namespace's traffic doesn't change which executions fire in another.

BUGGIFY applies only to the keys that you list. The `failpoints.buggify.keys` allowlist of the static
config is the whole selection: its entries are `{match, activationProbability, fireProbability,
error}`, `match` is a key name in which `*` matches any text, anywhere in the name (there are no
regular expressions), and the first entry that matches the name of a key sets its probabilities and
error. A key that matches no entry is never buggified.

What an activated key does when it fires depends on its entry and its call site:

- With an `error`, the key fails with that registered error (from `common/testing/failpointerrors`,
  such as `Unavailable`) wherever the code executes it with `failpoint.CheckError`.
- Without an `error`, the key fires at its `failpoint.ExecuteSwitch` call site. A switch branch
  selected for BUGGIFY must only make legal but unlikely states more likely, never produce one that
  cannot happen in production.
- Dynamic config settings of `serverbuggify.BuggifiableSettings` are named `dynamicconfig:<key>`.
  If an entry matches the name, the setting is activated with the entry's `activationProbability`
  and gets one of its alternative values, such as a batch size of 1, for the whole run.

### Configure it

Enable BUGGIFY on a server with failpoints enabled with the `failpoints.buggify` block of the static
server config (`config.FailpointsBuggify`). The server reads no environment variable for BUGGIFY and
no dynamic config setting changes it: to change the keys or the seed, restart the server. The block
is read once at startup, after all config files are loaded, and only if `failpoints.enabled` is
true, so with the switch off a malformed block does not stop the server:

```yaml
failpoints:
  enabled: true
  buggify:
    seed: 42     # A positive number enables BUGGIFY and needs keys; 0 or absent leaves it off.
    trace: true  # Print one line per decision, see "BUGGIFY trace". Default false.
    keys:        # The allowlist.
      - {match: "dynamicconfig:*", activationProbability: 0.25, fireProbability: 0.25}
```

### BUGGIFY trace

With `failpoints.buggify.trace: true`, the server prints one line to its standard output per BUGGIFY
decision, formatted by `serverbuggify.TraceLine`:

```text
BUGGIFY event=activation key="<failpoint name>" active=<true|false>
BUGGIFY event=fire key="<failpoint name>" scope="<scope>" execution=<n> action=<action>
```

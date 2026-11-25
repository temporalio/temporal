# Time Skipping

## Visibility search attributes

These predefined Boolean search attributes are available through both the Temporal CLI and tdbg:

- `TimeSkippingEnabled`: whether time skipping is currently enabled for the workflow. If it was enabled and is later disabled, either manually or automatically, this value becomes `false`.
- `TimeSkippingHasSkippedDuration`: the workflow has a positive accumulated skipped duration, including inherited skipped time. This means the workflow uses a virtual clock ahead of wall-clock time, even if time skipping has since been disabled.

Use Boolean conditions in `--query`. Both clients send the query to the server, which handles these fields; no client-specific flags or custom search attribute registration are required.

### Temporal CLI

List workflows that have skipped time and now have time skipping disabled:

```bash
temporal --namespace my-namespace workflow list \
  --query 'TimeSkippingEnabled = false AND TimeSkippingHasSkippedDuration = true'
```

Delete workflows that have skipped time:

```bash
temporal --namespace my-namespace workflow delete \
  --query 'TimeSkippingHasSkippedDuration = true' \
  --reason 'Delete workflows that skipped time'
```

Terminate workflows with time skipping enabled:

```bash
temporal --namespace my-namespace workflow terminate \
  --query 'TimeSkippingEnabled = true' \
  --reason 'Terminate workflows with time skipping enabled'
```

The Boolean attributes are also returned in workflow visibility search attributes. Use `--output json` to inspect them.

### tdbg

Use the admin visibility commands to list workflows that have skipped time or count workflows with time skipping enabled:

```bash
tdbg --namespace my-namespace visibility list \
  --query 'TimeSkippingHasSkippedDuration = true'

tdbg --namespace my-namespace visibility count \
  --query 'TimeSkippingEnabled = true'
```

### Missing values and refresh timing

Workflows without time-skipping information have neither attribute. They do not match `= false`; use `TimeSkippingEnabled IS NULL` to find them. Both clients support this condition.

Values refresh at workflow start, enabled-state changes, the first transition from zero to positive accumulated skipped duration, workflow close, and other visibility writes. Subsequent skips do not trigger extra visibility writes because `TimeSkippingHasSkippedDuration` remains `true`. Visibility updates are asynchronous.

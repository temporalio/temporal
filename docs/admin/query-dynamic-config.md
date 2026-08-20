# Inspect dynamic config with TDBG

Use `tdbg dynamic-config dump cvs` to dump the `ConfigValueMap` held by the dynamic config client in a running frontend process. The `dc` alias can be used in place of `dynamic-config`.

## Build TDBG

From the Temporal repository root:

```bash
make tdbg
```

## Dump all constrained values

```bash
./tdbg \
  --address 127.0.0.1:7233 \
  dc dump cvs
```

The command writes pretty-printed JSON in the current directory and prints the filename:

```text
tmp_dc_cvs_20260820T034405Z.json
```

The timestamp is UTC. The file contains configured values and their constraints; it does not contain registered code defaults or effective values. The command accepts dump responses up to 128 MiB.

## Current scope

- It dumps the in-memory dynamic config state of the frontend process receiving the RPC.
- The configured client must support returning its `ConfigValueMap`. The built-in file-based, static, and memory clients support it.
- It does not resolve effective values or explain how a value would be selected.

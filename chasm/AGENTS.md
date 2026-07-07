# CHASM Development Notes

## Adding a new library

When creating a new CHASM library under `chasm/lib/`:

1. Export a `NewNilLibrary() chasm.Library` function that returns a zero-value instance with nil task/component handlers. This is used by the shared tdbg registry (`chasm/lib/all`) to decode mutable state payloads without pulling in production dependencies.

2. Run the generator to update `chasm/lib/all/all.go`:
   ```
   go generate ./chasm/lib/all/...
   ```

The generator (`tools/gen-chasm-all`) scans `chasm/lib/` subdirectories for an exported `NewNilLibrary` function and emits the registration calls automatically.

# knowngaps — documentation fixtures (NOT enforced)

These packages document false-positive / false-negative cases the analyzer does
**not** handle yet. They are deliberately excluded from the running test suite
(`TestKnownGaps` is skipped) because the analyzer would not produce the desired
result today — they would fail.

Each file's header states the gap id, FP vs FN, current vs desired behavior, and
the effort to address it (see also plan.md). When a gap is implemented, move its
package into the relevant real test and delete it from here.

| package | gap | type | effort |
|---|---|---|---|
| `callarg` | proto literal passed to a marshaling callee escapes | FN | L (opaque marshal; param-retention facts) |
| `ifacemethod` | borrowed source behind an interface method | FN | S (annotate) / L (auto) |
| `funcvalue` | ownership through a function value / closure | FN | L (func-value signatures / points-to) |
| `nonsharedrecv` | receiver is a fresh local builder, not shared | FP | M–L |
| `immutablefield` | receiver field is immutable after init | FP | S (annotate) / L (auto) |
| `conditionalclone` | clone on one branch only (flow-insensitive) | FP/FN | L (flow-sensitive) |
| ~~`bytetoken`~~ | immutable `[]byte` token field | FP | RESOLVED: `[]byte` is no longer a hazard leaf (noise reduction B); see the `byteslice` fixture |

# Replay diagnostics

This standard-library-only server package is active under `gomad && wasip1 &&
wasm`. Ordinary builds and native Gomad use the complementary no-op API. Declare
probes, dumps and invariants during package initialization; registration closes
before `main`. A failed, duplicate, late, invalid or capacity-exceeding declaration
poisons the retained registry even if the application recovers its panic.

```go
var event = gomaddebug.DeclareProbe("workflow.event")

func observe(id int64, existingName string) {
    if event.Enabled() {
        event.Emit(gomaddebug.Int64(id), gomaddebug.String(existingName))
    }
}
```

Evaluate fields inside the guard. Fields accept signed/unsigned integers,
booleans, float64 and existing strings; no interface arguments or formatting are
involved. The fixed 16KiB encoder drops oversized records and emits a cumulative
drop count. Guest records contain no time or target lookup: the host attaches
its already-held instance/incarnation, tape choice, trace and virtual-time
coordinates. Registry IDs follow global declaration order, starting at zero;
the limit is 256 entries with names of at most 128 ASCII name bytes.

`RegisterDump(name, func(*DumpWriter))` runs only on the runtime debug goroutine
after the prefix is sealed and the application is frozen. The writer retains at
most 64KiB per callback. Panic and truncation are reported independently, so
other selected callbacks still run. Never acquire an application lock or wait
for an application message. A nonyielding callback is terminated by the host
inspection deadline; simulation execution cannot resume afterward.

`RegisterInvariant(name, monotonic, func() bool)` retains the declaration used
by replay preflight. Ascending scan permits recovering predicates; explicit
bisect relies on the declared once-failing-always-failing contract. Callback
panic is an evaluation error, not a predicate result.

The Temporal dumps `temporal.shards` and `temporal.mutable_states` keep the last
32 shard write-unlock and 64 mutable-state mutation snapshots. They report the
snapshot sequence and mutation seam, with explicit stale-view and identity
truncation labels. They contain scalar/byte copies, never live shard, workflow
or cache references. The corresponding `temporal.*.snapshot` probes are guarded
before field evaluation. The host's resolved inspection boundary supplies the
boundary at which those completed snapshots are read; a snapshot sequence is
not a tape ordinal.

## Runtime seam and record encoding

The active source links only these runtime functions: `gomadDebugRegisterLibrary`,
`gomadDebugRegister`, `gomadDebugRegistryClosed`, `gomadDebugEmit`. Runtime stores
the callback pointers and fixed four-word mask without importing this package.
Descriptor kinds are 3 dump, 4 invariant, 5 probe; flag bit 0 declares monotonic
failure. Registry failure sends sticky incomplete closure.

Each emit is one complete record: four little-endian uint32 words containing
kind, declaration ID, status/count, payload byte length, followed by payload.

| Kind | Status/count | Payload |
| --- | --- | --- |
| 1 probe | Field count | Ordered fields: type byte, LE uint32 byte length, bytes |
| 2 dump | 0 success, 1 panic, 2 truncated | Retained dump bytes |
| 3 invariant | 0 true, 1 false, 2 panic | Empty |
| 4 dropped | 0; ID is 0 | LE uint64 cumulative dropped records |

Field types are 1 int64 (LE8), 2 uint64 (LE8), 3 bool (one byte 0/1),
4 float64 (IEEE754 LE8), 5 string (existing raw bytes). The host validates
lengths/types and bounds transport output independently.

`testdata/fixture` is a real guest fixture: its dormant binary retains probes,
panic/truncated/allocating dumps, monotonic and recovering invariants, and an
application lock held across scheduler decisions. Enabling `fixture.allocating`
adds a heap allocation without changing stdout or forcing GC; the compulsory
runtime witness must reject it. The fixture deliberately exits 1.

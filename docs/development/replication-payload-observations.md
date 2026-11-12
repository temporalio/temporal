# Replication payload route observations

`PayloadObservation.Source` and `Target` identify the native cluster/shard keys
for a tracked HIGH-priority payload. `Origin` retains source cluster name, shard
and task ID. Source and target shard IDs can differ, so a consumer cannot infer
the receiving shard from source origin or assume matching shard counts.

Both receiver implementations stamp the route on tracked receipt observations.
The standard executable converter captures a scoped observer for each conversion
call, so successful execution retains the same route even when a converter serves
concurrent streams. The shared converter and its original toolbox are not mutated.
When no observer is registered, conversion uses the original converter without a
scoped copy or route wrapper.

Custom executable converters must preserve the source/target pair through task
completion. Empty route keys do not establish receiving placement. Consumers
should treat missing or contradictory metadata as unavailable attribution rather
than guess a route. These keys do not prove acquired ownership or identify an
acquired persistence range.

Existing lifecycle semantics remain: receipt excludes tracker duplicates,
successful execution is distinct from a wire ACK, and cancellation/failure does
not count as success. No event export, quota admission, wire field or persistence
format is introduced. Callbacks remain concurrent, in-memory and nonblocking.

Tests cover differing source/target shards in default, isolated and legacy paths,
duplicate suppression, matched receipt/completion keys, concurrent conversion
routes and the disabled-observer path.

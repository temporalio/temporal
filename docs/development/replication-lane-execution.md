# Shared replication lane execution

The lane sender can use `history.ReplicationStreamSenderLaneScheduler=pooled`
to share execution workers across isolated HIGH-priority lanes. The default is
`per-lane`; unknown values also select per-lane execution. Changing the effective
scheduler invalidates the stream so it reconnects with the selected executor.

Each pooled stream has two preparation workers, one flow-control worker and one
writer, coordinated by one event loop. The existing default HIGH/LOW send paths
remain separate. Logical lane count does not create additional pooled workers.
There are at most sixteen scheduling classes and two retained preparation/payload
slots per class. Slot occupancy includes work being prepared, not just completed
payloads. Conversion retries release their preparation slot while retaining the
lane's cursor lease and retry identity.

`DesiredLane.SchedulingClass` selects an execution group, with zero as the default.
The class is not persisted and does not grant traffic. Policies retain rate and
budget authority through their `LaneLimiter` implementations. An optional
`LaneLimiterReadiness` probe can avoid waiting for flow control on ineligible work;
it must not spend or hold credit. The sender rechecks actual admission after flow
control, immediately before dispatching the write. Rate-only policy evaluations
wake the executor even when lane membership does not change.

The lane registry remains responsible for cursor ownership, fixed retirement
prefixes and ACK progress. Work holds a registry lease through conversion and
send. A committed send advances its cursor before releasing that lease, including
when PAUSE or a class change arrives during the write. Invalidated prepared work
is discarded without advancing its cursor. Shutdown stops the stream before
joining workers so a blocked stream writer can unwind.

Unit/race coverage exercises worker and slot bounds, slow conversion, class
changes, nonspending readiness, late admission refusal, retries, fixed retirement
scope, concurrent PAUSE, committed writes, panic handling and shutdown. The
SQLite XDC suite includes a pooled fixture and a scheduler-switch test using a
backlog. These tests establish execution and lifecycle behavior, not a production
throughput or observation-overhead qualification.

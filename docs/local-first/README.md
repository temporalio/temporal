# Local-first specification index

These documents are the durable memory for the local-first project. They separate the approved
delivery plan from contracts that will evolve as the prototype is implemented.

| Document | Purpose |
| --- | --- |
| [`plan.md`](plan.md) | Approved project plan, phases, and longer-term alternatives |
| [`architecture.md`](architecture.md) | Component responsibilities, trust boundaries, and repository ownership |
| [`semantics.md`](semantics.md) | Normative user-visible behavior and distributed-system invariants |
| [`protocol.md`](protocol.md) | RPC, persistence, cursor, lease, and state-transition contracts |
| [`decisions.md`](decisions.md) | Current accepted decisions and unresolved questions |
| [`test-plan.md`](test-plan.md) | Acceptance matrix and cross-repository verification strategy |
| [`spike-namespace-history-layer.md`](spike-namespace-history-layer.md) | Namespace promotion findings and selected active-History path |
| [`spike-atomic-synchronization.md`](spike-atomic-synchronization.md) | Whole-delta persistence findings and selected single-update path |
| [`in-process-local-server.md`](in-process-local-server.md) | Deferred Go/WASM in-process server plan and Rust/Purego fallback |
| [`review.md`](review.md) | Brief implementation decisions, limitations, and questions for review |
| [`demo.md`](demo.md) | Command and expected milestones for the runnable steel thread |
| [`run-core-demo.sh`](run-core-demo.sh) | Builds and runs the standalone-server plus raw-Core demo |
| [`run-agent-demo.sh`](run-agent-demo.sh) | Runs the interactive coding-agent demo and smoke acceptance path |

## Maintenance rules

- Use **MUST**, **MUST NOT**, **SHOULD**, and **MAY** for normative requirements.
- Give new decisions stable `D###` identifiers and tests stable `T###` identifiers.
- Do not repurpose identifiers. Remove reverted decisions instead of retaining contradictory
  entries; leave identifier gaps and rely on Git history for discarded rationale.
- Protocol examples are logical schemas until source protos assign field numbers and concrete
  package locations.
- Keep speculative production work clearly separated from prototype requirements.
- Reconcile contradictions immediately. Current accepted decisions govern, followed by the
  normative semantic and protocol specifications, then the original plan.

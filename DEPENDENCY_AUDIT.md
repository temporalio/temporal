# Dependency Audit: Supply Chain Risk Reduction Plan

**Date:** 2026-03-28
**Scope:** All 68 direct dependencies in `go.mod` (+ 89 indirect)
**Goal:** Minimize supply chain attack surface; make remaining deps easy to upgrade

---

## Executive Summary

The Temporal server has **68 direct** and **89 indirect** Go dependencies (579 go.sum entries).
This plan categorizes every direct dependency by removal lift (S/M/L/XL), supply chain risk,
and recommends a prioritized action for each.

**Key wins available:**
- **11 dependencies removable with Small effort** (< 1 day each)
- **7 dependencies removable with Medium effort** (1-3 days each)
- **3 dependencies consolidatable** (dual libs serving the same purpose)
- Removing all "Small" items eliminates ~15 direct deps and their transitive trees

---

## Risk Rating Legend

| Risk | Meaning |
|------|---------|
| HIGH | Unmaintained, single-maintainer, Temporal-forked, or brings massive transitive tree |
| MED  | Well-maintained but large attack surface or niche maintainership |
| LOW  | Widely-used, well-maintained, small transitive footprint |

## Lift Rating Legend

| Lift | Meaning |
|------|---------|
| S    | < 1 day. Drop-in stdlib replacement or trivial wrapper removal |
| M    | 1-3 days. Moderate refactoring, < 10 files |
| L    | 1-2 weeks. Significant refactoring, 10-50 files |
| XL   | > 2 weeks or impractical. Core infrastructure |

---

## TIER 1: REMOVE — Small Lift, Clear Win

These can be removed with stdlib or trivial inline code. **Do these first.**

### 1. `github.com/pkg/errors` (indirect, 1 file)
- **Risk:** HIGH (deprecated, unmaintained since 2021)
- **Lift:** S
- **Action:** Replace the single usage in `common/util/error_type.go` with `fmt.Errorf` / `errors.As`. Remove from `go.mod`.
- **Transitive deps removed:** 0 (leaf)

### 2. `gopkg.in/validator.v2` (1 file)
- **Risk:** MED (old, low-activity)
- **Lift:** S
- **Action:** Replace with hand-written validation in `common/config/`. It's one file with simple struct tag validation. Stdlib `reflect` or explicit checks suffice.
- **Transitive deps removed:** 0 (leaf)

### 3. `github.com/iancoleman/strcase` (4 files)
- **Risk:** LOW (small lib, single maintainer)
- **Lift:** S
- **Action:** Inline a 15-line `toSnakeCase()` function. Used only for DB column name mapping in MySQL/PostgreSQL/SQLite SQL plugins.
- **Transitive deps removed:** 0 (leaf)

### 4. `github.com/go-faker/faker/v4` (1 file)
- **Risk:** MED (niche, test-only)
- **Lift:** S
- **Action:** Replace with hand-written test fixtures in `common/testing/fakedata/`. Only one file uses it. `math/rand` + literals suffice.
- **Transitive deps removed:** 0 (leaf)

### 5. `github.com/olekukonko/tablewriter` (1 file)
- **Risk:** LOW (single maintainer)
- **Lift:** S
- **Action:** Replace with `fmt.Fprintf` + `text/tabwriter` (stdlib). Used only in `tools/tdbg/` for CLI table output.
- **Transitive deps removed:** 1 (`github.com/mattn/go-runewidth`)

### 6. `github.com/fatih/color` (4 files)
- **Risk:** LOW (popular, well-maintained)
- **Lift:** S
- **Action:** Replace with direct ANSI escape codes or stdlib. Used only in `tools/tdbg/` and `cmd/tools/protogen/`. These are developer tools, not production paths.
- **Transitive deps removed:** 2 (`mattn/go-colorable`, `mattn/go-isatty`)

### 7. `github.com/maruel/panicparse/v2` (1 file)
- **Risk:** MED (niche, single maintainer)
- **Lift:** S
- **Action:** Remove from `tools/testrunner/`. Panic output formatting is nice-to-have, not essential. Use raw stack traces.
- **Transitive deps removed:** 0 (leaf)

### 8. `github.com/sony/gobreaker` (2 files)
- **Risk:** LOW (small, stable)
- **Lift:** S
- **Action:** Inline a simple circuit breaker (~50 lines). Used in `common/circuitbreaker/` wrapping gobreaker with dynamic settings. The wrapper is already most of the code.
- **Transitive deps removed:** 0 (leaf)

### 9. `github.com/blang/semver/v4` (8 files)
- **Risk:** LOW (stable, no updates needed)
- **Lift:** S-M
- **Action:** Replace with `golang.org/x/mod/semver` (quasi-stdlib) or a small inline parser. Used for schema version comparison in persistence layer.
- **Transitive deps removed:** 0 (leaf)

### 10. `github.com/google/go-cmp` (6 files)
- **Risk:** LOW (Google-maintained)
- **Lift:** S
- **Action:** Replace with `reflect.DeepEqual` or testify assertions (already used everywhere). Only 6 test files use `cmp.Compare`.
- **Transitive deps removed:** 0 (leaf)

### 11. `github.com/mitchellh/mapstructure` (2 files)
- **Risk:** MED (Mitchell Hashimoto stepped back from maintenance)
- **Lift:** S
- **Action:** Replace with `encoding/json` round-trip (`marshal map -> unmarshal struct`) or explicit field mapping in `common/dynamicconfig/`. Only 2 files.
- **Transitive deps removed:** 0 (leaf)

**Estimated total effort for Tier 1: ~3-5 engineer-days**
**Dependencies removed: 11 direct + ~5 transitive**

---

## TIER 2: REMOVE — Medium Lift, Worth Doing

### 12. `github.com/Masterminds/sprig/v3` (1 file)
- **Risk:** HIGH (massive transitive tree: Masterminds/goutils, Masterminds/semver, huandu/xstrings, shopspring/decimal, spf13/cast, mitchellh/copystructure, mitchellh/reflectwalk)
- **Lift:** M
- **Action:** Used only in `common/config/loader.go` for YAML template functions. Replace with a hand-picked subset of template funcs (env, default, required). This alone removes **7 transitive dependencies**.
- **Transitive deps removed:** 7

### 13. `github.com/robfig/cron/v3` (1 file)
- **Risk:** LOW (stable, but pulls in `robfig/cron` v1 as well via ringpop)
- **Lift:** M
- **Action:** Inline cron schedule parser. Used only in `common/backoff/cron.go` for `cron.ParseStandard()`. A focused cron parser is ~100 lines.
- **Transitive deps removed:** 1 (`robfig/cron` v1, if ringpop dep is also addressed)

### 14. `github.com/emirpasic/gods` (6 files)
- **Risk:** LOW (stable, popular)
- **Lift:** M
- **Action:** Replace `TreeMap` usage in `service/matching/` with stdlib `sort.Slice` on a slice, or use `github.com/tidwall/btree` which is already a dependency. Consolidate to one ordered-collection library.
- **Transitive deps removed:** 0 (but reduces surface)

### 15. `github.com/tidwall/btree` (1 file)
- **Risk:** LOW (single maintainer but quality code)
- **Lift:** M
- **Action:** If `gods` TreeMap is replaced with btree, keep btree. Otherwise, consolidate both into one. Alternatively, a stdlib-based sorted slice suffices for the matching service queue sizes.
- **Transitive deps removed:** 0 (leaf)

### 16. `github.com/urfave/cli` v1 (5 files)
- **Risk:** MED (legacy v1, alongside v2)
- **Lift:** M
- **Action:** Migrate `tools/sql/` and `tools/common/schema/` from cli/v1 to cli/v2 (already used in 26 files). Eliminates the dual-version dependency.
- **Transitive deps removed:** 1 (cli v1)

### 17. `github.com/gorilla/mux` (9 files)
- **Risk:** MED (gorilla project went through maintenance crisis in 2022-2023; now community-maintained)
- **Lift:** M
- **Action:** Replace with Go 1.22+ `net/http.ServeMux` which now supports method-based routing and path parameters. Used in frontend HTTP server and Nexus handlers. ~9 files.
- **Transitive deps removed:** 0 (leaf)

### 18. `github.com/dgryski/go-farm` (24 files)
- **Risk:** MED (single maintainer, hash function)
- **Lift:** M (but with caveats)
- **Action:** Replace with `maphash` (stdlib Go 1.24+) or `xxhash`. **CRITICAL CAVEAT:** Hash values are used for consistent routing and stored task IDs. Changing the hash function requires a migration strategy. Start by wrapping in an interface so the algorithm can be swapped.
- **Transitive deps removed:** 0 (leaf)

**Estimated total effort for Tier 2: ~8-12 engineer-days**
**Dependencies removed: 7 direct + ~9 transitive**

---

## TIER 3: CONSOLIDATE — Reduce Redundancy

### 19. `github.com/uber-go/tally/v4` + `github.com/cactus/go-statsd-client/v5` (13 files total)
- **Risk:** MED (tally is legacy metrics path)
- **Lift:** L
- **Action:** Complete migration to OpenTelemetry. Tally (10 files) and StatsD (3 files) are the legacy metrics pipeline. OTel already handles Prometheus export. Once OTel migration is complete, remove both + their transitive deps.
- **Transitive deps removed:** 3 (`cactus/go-statsd-client/statsd`, `rcrowley/go-metrics`, `twmb/murmur3`)

### 20. `github.com/lib/pq` + `github.com/jackc/pgx/v5` (2 files total)
- **Risk:** LOW (both well-maintained)
- **Lift:** M
- **Action:** Standardize on `pgx` only. `lib/pq` is the legacy PostgreSQL driver (1 file). Remove the `pq` driver option and keep only `pgx`.
- **Transitive deps removed:** 0 (but removes a driver binary from builds)

### 21. `github.com/golang-jwt/jwt/v4` + `github.com/go-jose/go-jose/v4` (5 files total)
- **Risk:** LOW (both well-maintained)
- **Lift:** M
- **Action:** `go-jose` can handle JWT parsing (it's a superset). Consolidate auth code in `common/authorization/` to use only `go-jose`. Removes `golang-jwt`.
- **Transitive deps removed:** 0 (leaf)

**Estimated total effort for Tier 3: ~5-8 engineer-days**
**Dependencies removed: 4 direct + ~3 transitive**

---

## TIER 4: KEEP — Large Lift or Essential, Focus on Pinning & Monitoring

These are either core infrastructure, high-file-count, or not worth replacing. Strategy: **pin versions, monitor advisories, automate updates.**

### Core Protocol & Serialization (KEEP, XL to replace)
| Dependency | Files | Notes |
|---|---|---|
| `google.golang.org/grpc` | 218 | Core RPC transport. No alternative. |
| `google.golang.org/protobuf` | 490 | Core serialization. No alternative. |
| `go.temporal.io/api` | 1,054 | First-party. Inherent coupling. |
| `go.temporal.io/sdk` | 154 | First-party. Primarily test use. |
| `github.com/nexus-rpc/sdk-go` | 64 | First-party adjacent. Active feature. |
| `github.com/grpc-ecosystem/grpc-gateway/v2` | 3 | Minimal surface; needed for HTTP API. |

### Database Drivers (KEEP, essential for persistence backends)
| Dependency | Files | Notes |
|---|---|---|
| `github.com/jmoiron/sqlx` | 19 | Core SQL abstraction. Well-maintained. |
| `github.com/jackc/pgx/v5` | 1 | Modern PG driver. Keep. |
| `github.com/go-sql-driver/mysql` | 2 | MySQL driver. Keep. |
| `github.com/gocql/gocql` | 22 | Cassandra driver. Keep if Cassandra supported. |
| `modernc.org/sqlite` | 2 | Pure-Go SQLite. Keep for dev/test. |

### Temporal Forks (KEEP, monitor closely)
| Dependency | Files | Risk | Notes |
|---|---|---|---|
| `github.com/temporalio/ringpop-go` | 6 | HIGH | Forked gossip protocol. Core to membership. Long-term: consider replacing with custom membership or etcd-based discovery. |
| `github.com/temporalio/tchannel-go` | 3 | HIGH | Transport for ringpop only. Removing ringpop removes this. |
| `github.com/temporalio/sqlparser` | 46 | HIGH | Forked SQL parser for visibility queries. 46 files deeply coupled. Long-term: consider building a purpose-built query DSL parser instead of forking a general SQL parser. |

### Observability (KEEP, well-maintained ecosystem)
| Dependency | Files | Notes |
|---|---|---|
| `go.opentelemetry.io/otel` (+ sub-pkgs) | 42 | Industry standard. 10 sub-packages but single ecosystem. |
| `go.opentelemetry.io/collector/pdata` | - | OTel data types. |
| `github.com/prometheus/client_golang` | 3 | Needed for Prometheus export. |
| `github.com/prometheus/client_model` | - | Prometheus data model. |
| `github.com/prometheus/common` | - | Prometheus utilities. |
| `go.uber.org/zap` | 13 | Structured logging. Deeply embedded. |

### Dependency Injection & Error Handling (KEEP)
| Dependency | Files | Notes |
|---|---|---|
| `go.uber.org/fx` | 92 | DI framework. 92 files. Would be XL to replace. |
| `go.uber.org/multierr` | 14 | Tiny, no transitive deps. |

### Testing (KEEP, test-only)
| Dependency | Files | Notes |
|---|---|---|
| `github.com/stretchr/testify` | 771 | Test-only. Universal Go testing lib. |
| `go.uber.org/mock` | 410 | Test-only. Mock generation. |

### Utilities (KEEP, low risk)
| Dependency | Files | Notes |
|---|---|---|
| `github.com/google/uuid` | 246 | Google-maintained, tiny, no deps. |
| `gopkg.in/yaml.v3` | 9 | Canonical YAML lib. |
| `github.com/olivere/elastic/v7` | 17 | ES client. Keep if ES supported. Consider migration to official ES client. |

### stdlib-adjacent (KEEP, trusted)
| Dependency | Files | Notes |
|---|---|---|
| `golang.org/x/exp` | - | Quasi-stdlib. |
| `golang.org/x/oauth2` | - | Quasi-stdlib. |
| `golang.org/x/sync` | - | Quasi-stdlib. |
| `golang.org/x/text` | - | Quasi-stdlib. |
| `golang.org/x/time` | - | Quasi-stdlib. |
| `google.golang.org/api` | - | GCP API client. Keep if GCS archiver supported. |

### Cloud SDKs (KEEP or make optional)
| Dependency | Files | Notes |
|---|---|---|
| `cloud.google.com/go/storage` | 4 | GCS archiver. Consider build-tag isolation. |
| `github.com/aws/aws-sdk-go` | 8 | S3 archiver. Consider build-tag isolation. **Note:** This is AWS SDK v1 (deprecated). Should migrate to v2 when keeping. |

### CLI Tools (KEEP)
| Dependency | Files | Notes |
|---|---|---|
| `github.com/urfave/cli/v2` | 26 | CLI framework for tdbg/tools. Keep after v1 migration. |
| `github.com/jstemmer/go-junit-report/v2` | 4 | Test tooling only. |

---

## TIER 5: STRUCTURAL IMPROVEMENTS — Reduce Blast Radius of Remaining Deps

These are not about removing dependencies but about **making what remains safer and easier to upgrade.**

### A. Build-Tag Isolation for Optional Backends
- **Action:** Put Cassandra (`gocql`), Elasticsearch (`olivere/elastic`), GCS (`cloud.google.com/go/storage`), S3 (`aws/aws-sdk-go`), and MySQL/PostgreSQL drivers behind build tags.
- **Benefit:** Default builds pull in far fewer dependencies. Only the backends you deploy get compiled in.
- **Lift:** L (2-3 weeks). Requires refactoring persistence plugin registration.

### B. Pin + Automate Dependency Updates
- **Action:** Use Dependabot or Renovate with:
  - Automerge for patch versions of LOW-risk deps
  - Required review for minor/major bumps
  - Block list for known-vulnerable transitive deps
- **Benefit:** Deps stay current, reducing window for known CVEs.

### C. Vendor Dependencies
- **Action:** `go mod vendor` and commit the vendor directory.
- **Benefit:** Builds are reproducible even if upstream is compromised or deleted. You review diffs on every dependency change.
- **Trade-off:** Larger repo, but standard practice for high-security Go projects.

### D. Replace `olivere/elastic/v7` with Official Client
- **Risk:** `olivere/elastic` is community-maintained, not official Elastic.
- **Lift:** L (17 files, custom query builders)
- **Action:** Migrate to `github.com/elastic/go-elasticsearch/v8` (official).

### E. Migrate `aws/aws-sdk-go` v1 → v2
- **Risk:** AWS SDK v1 is in maintenance mode.
- **Lift:** M (8 files)
- **Action:** Migrate to `github.com/aws/aws-sdk-go-v2`. Smaller dependency tree, modular imports.

### F. Long-term: Replace Ringpop
- **Risk:** Forked, unmaintained upstream, depends on tchannel-go and Apache Thrift.
- **Lift:** XL (architectural change)
- **Action:** Evaluate replacing the gossip-based membership with etcd/consul-based service discovery or a custom SWIM implementation without tchannel. This removes ringpop + tchannel + thrift + several indirect deps.

---

## Prioritized Execution Order

| Phase | Items | Effort | Deps Removed | Risk Reduction |
|-------|-------|--------|--------------|----------------|
| **Phase 1** | Tier 1 (items 1-11) | 3-5 days | ~16 | Quick wins, no architectural changes |
| **Phase 2** | Tier 2 (items 12-18) | 8-12 days | ~16 | Moderate refactoring, high value (especially sprig) |
| **Phase 3** | Tier 3 (items 19-21) | 5-8 days | ~7 | Consolidation, cleaner architecture |
| **Phase 4** | Tier 5A (build tags) | 10-15 days | 0 (but isolates ~30) | Massive blast radius reduction |
| **Phase 5** | Tier 5D-E (client migrations) | 5-10 days | 0 (but modernizes) | Removes deprecated SDK risk |
| **Phase 6** | Tier 5F (ringpop replacement) | 30+ days | ~5 | Removes highest-risk forked deps |

**Total removable with Phases 1-3: ~39 dependencies (direct + transitive)**
**Timeline estimate: Phases 1-3 achievable in ~4-5 weeks of focused work.**

---

## Dependency Count Impact

| State | Direct | Indirect | go.sum lines |
|-------|--------|----------|--------------|
| Current | 68 | 89 | 579 |
| After Phase 1 | 57 | ~84 | ~550 |
| After Phase 2 | 50 | ~75 | ~510 |
| After Phase 3 | 46 | ~72 | ~490 |
| After build-tag isolation | 46 (but ~16 optional) | ~72 | ~490 |

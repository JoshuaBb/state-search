# ADR 003 — Runtime-checked SQL (no compile-time SQLx macros)

**Status:** Accepted

---

## Context

SQLx offers two modes for query checking:

1. **Compile-time** (`query!`, `query_as!` macros): the macro connects to a live database during `cargo build` (or reads a cached `.sqlx/` snapshot) to verify SQL and infer column types. Catches typos and schema mismatches at compile time.

2. **Runtime** (`sqlx::query`, `sqlx::query_as`): SQL is validated and types are checked when the query runs. No live database needed to compile.

---

## Decision

Use runtime-checked SQL throughout. Do not use `query!` or `query_as!` compile-time macros.

---

## Reasoning

**CI and local builds don't require a live database.** Compile-time macros need either a running Postgres instance or a committed `.sqlx/` cache directory. Keeping the cache in sync adds friction every time a query or migration changes, and requiring a database during CI complicates the build pipeline.

**Migrations run at startup.** The schema is always up to date by the time queries run. Runtime type mismatches surface quickly in tests and local dev rather than silently diverging from a stale cache.

**The codebase is small.** The safety benefit of compile-time checking is highest in large codebases with many contributors. For a project this size the operational cost outweighs the benefit.

---

## Consequences

**Good:**
- `cargo build` works without a database.
- No `.sqlx/` cache directory to maintain.
- Migrations and queries can be changed together in one commit without a rebuild step.

**Bad:**
- SQL typos and schema mismatches aren't caught until runtime.
- Column type inference is not available — `FromRow` structs must match the query manually.
- IDEs can't provide SQL completion or validation inside query strings.

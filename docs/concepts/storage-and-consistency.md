# Storage and consistency

Ketebe separates authoritative durable state from derived retrieval acceleration structures.

## Durable state

Acknowledged mutations are backed by Ketebe's durable storage path. The implementation uses write-ahead logging and persisted segment state so that a process restart can reconstruct acknowledged data.

Persisted data and recovery metadata are authoritative. ANN indexes and caches are derived state: they may be persisted for faster startup, but correctness must not depend on an unrecoverable index file.

## Recovery

On restart, Ketebe validates persisted state and replays the durable mutation history required to reconstruct current state. Corruption is treated as an explicit error condition rather than silently skipped when doing so could produce incorrect results.

## Visibility

A successful write is expected to become visible to subsequent reads according to the product's acknowledged-write semantics. Retrieval acceleration must not silently make a newly acknowledged mutation disappear from the logical query result solely because an index refresh lags behind durable state.

## Updates and deletes

Updates and deletes participate in ordered state resolution. Newer mutations supersede older record versions; deletes prevent older versions from reappearing in retrieval results.

## Derived indexes

Exact retrieval is a useful correctness reference. HNSW and other acceleration structures are rebuildable derivatives of authoritative record state.

This separation has two operational consequences:

- losing an index may reduce performance while it is rebuilt, but must not lose acknowledged records;
- storage and recovery testing is more important than treating index files as the database source of truth.

## Deployment evolution

Public data and query semantics are intended to remain independent of deployment topology. As Ketebe evolves, implementation details may change without requiring applications to understand WAL layout, segment formats, or internal index lifecycle.
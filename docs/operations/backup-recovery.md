# Backup and recovery

Backup is useful only when restore has been verified.

Ketebe's durable state is designed around persisted storage plus the recovery metadata needed to reconstruct acknowledged data. Derived ANN indexes are not the sole source of truth and can be rebuilt when necessary.

## Backup goals

Define the recovery objective before choosing a backup schedule:

- how much acknowledged data loss is acceptable,
- how long recovery may take,
- whether credentials and external provider configuration are backed up separately,
- whether the restore target is the same environment or a new one.

## What to protect

Protect the complete authoritative Ketebe data directory and the metadata required to interpret it consistently. Do not build a backup procedure around copying only index artifacts.

## Restore validation

A recovery drill should verify more than process startup:

1. restore into an isolated environment,
2. start Ketebe against the restored state,
3. verify collection metadata and record counts,
4. run representative dense, sparse, hybrid, and filtered queries,
5. verify writes after restore,
6. confirm authentication and authorization behavior,
7. record recovery duration and any manual steps.

## Consistency of copies

A filesystem copy taken while storage is actively mutating may not represent a valid recovery point unless the underlying deployment provides a supported consistent snapshot boundary. Use the release-specific backup procedure once packaged deployment guidance is published.

## Index rebuilds

A restore may require rebuilding derived retrieval structures. Capacity plans should account for temporary CPU, memory, I/O, and latency effects during rebuild or warm-up.
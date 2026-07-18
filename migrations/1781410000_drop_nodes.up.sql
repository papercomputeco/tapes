-- Retire the persisted merkle node layer.
--
-- The deriver never read this table: every derive pass rebuilds the
-- merkle DAG in memory from raw_turns and writes the sessions/traces/
-- spans projection. The nodes table was a redundant write sink that
-- double-stored each payload (bucket + content). No production path
-- reads or writes it anymore (the node-DAG storage surface, ingest/
-- derive node writes, and the Kafka node-event publish were all
-- removed), so dropping it is behaviour-preserving for the derived read
-- model — proven by the byte-identical span/trace corpus gate.
--
-- ancestry_chains_rows only ever walked this table and has no remaining
-- caller. spans.node_hash is intentionally kept: it is the merkle
-- message-identity hash sourced from the retained in-memory merkle layer
-- (merkle.ProjectContent) and carries no foreign key to this table. It is
-- reserved for v1.1 payload dedup (a derived blob table keyed on this hash)
-- and is deliberately not exposed on the wire.

DROP FUNCTION IF EXISTS ancestry_chains_rows(text[], integer);

DROP TABLE IF EXISTS nodes;

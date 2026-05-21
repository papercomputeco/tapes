-- Inverse of 1779329142_session_tracking.up.sql. Order matters because
-- nodes.session_id and sessions.parent_session_id both reference
-- sessions(id) — the FK column must come off nodes before the table
-- can drop. The composite PK (org_id, hash) reverts to the baseline
-- single-column PK on hash, and the dropped self-referential FK on
-- parent_hash is restored so the down state matches baseline byte-for-
-- byte.
--
-- Cross-org dedup before PK restore: under the composite PK two rows
-- may share `hash` across different `org_id` values. Restoring a
-- single-column PK on `hash` would fail on those duplicates and leave
-- the schema half-migrated. Collapse each hash bucket to one row,
-- preferring the nil-UUID sentinel row (legacy, non-session writers)
-- when present, otherwise the row with the smallest `ctid` for a
-- deterministic survivor. This DROPS data on any hash that has
-- diverged across orgs; the down direction is destructive by design.

DROP INDEX IF EXISTS nodes_hash_idx;

DELETE FROM nodes a
 USING nodes b
 WHERE a.hash = b.hash
   AND (
        (b.org_id = '00000000-0000-0000-0000-000000000000'::uuid
         AND a.org_id <> '00000000-0000-0000-0000-000000000000'::uuid)
     OR (
        (b.org_id = '00000000-0000-0000-0000-000000000000'::uuid)
          = (a.org_id = '00000000-0000-0000-0000-000000000000'::uuid)
        AND a.ctid > b.ctid
     )
   );

ALTER TABLE nodes
    DROP CONSTRAINT IF EXISTS nodes_pkey;

ALTER TABLE nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (hash);

ALTER TABLE nodes
    ADD CONSTRAINT nodes_parent_hash_fkey
        FOREIGN KEY (parent_hash) REFERENCES nodes(hash) ON DELETE SET NULL;

ALTER TABLE nodes
    DROP COLUMN IF EXISTS org_id;

DROP INDEX IF EXISTS nodes_session_idx;

ALTER TABLE nodes
    DROP COLUMN IF EXISTS session_id;

DROP INDEX IF EXISTS sessions_parent_idx;
DROP INDEX IF EXISTS sessions_auth_subject_idx;
DROP INDEX IF EXISTS sessions_org_lastseen_idx;

DROP TABLE IF EXISTS sessions;

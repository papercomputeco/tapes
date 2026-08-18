-- Retire the storage behind the extracted surfaces.
--
-- Skills and span search have moved to their cassettes, which own their
-- data in their own schemas (`skills.*`, `search.*`) with their own
-- migrations. Core's copies of the routes, handlers, packages, and the
-- embed worker are removed in this same change, so nothing in this
-- repository reads or writes these tables anymore:
--
--   * `skills` / `skill_versions` — created by 1781320000/1781330000 and
--     reshaped through 1781350000; served core's /v1/skills surface. The
--     skills cassette migrated this data into its own schema before the
--     client cutover.
--   * `span_embeddings` / `span_embeddings_failures` — never created by a
--     migration: the retired spanembed store issued CREATE TABLE IF NOT
--     EXISTS at runtime, so long-lived deployments have them and fresh
--     ones may not. The search cassette re-embedded from the span
--     projection into its own schema; these vectors are derivable data,
--     not capture.
--   * `tapes_embeddings` — an out-of-band predecessor of span_embeddings
--     observed in long-lived deployments; created by no migration and
--     read by nothing.
--
-- 1781490000's single-tenant assertion names skills/skill_versions in its
-- fixed_tables array; it guards every check with to_regclass, so it runs
-- unchanged whether or not these tables exist and is deliberately not
-- amended (it has already been applied everywhere).

DROP TABLE IF EXISTS skill_versions;
DROP TABLE IF EXISTS skills;
DROP TABLE IF EXISTS span_embeddings_failures;
DROP TABLE IF EXISTS span_embeddings;
DROP TABLE IF EXISTS tapes_embeddings;

-- ListSessionsByHarnessSessionID (GET /v1/sessions?harness_session_id=...)
-- filters sessions WHERE org_id = $1 AND harness_session_id = $2. The only
-- index carrying harness_session_id is the natural-key unique constraint
-- (org_id, harness_id, harness_session_id), whose btree cannot seek without
-- a harness_id between the leading columns — so the lone-id filter degrades
-- to a scan as the table grows. A btree on exactly the query's predicate
-- makes it the point lookup it reads as.
-- Note: CONCURRENTLY omitted because golang-migrate wraps each file in a
-- transaction; CREATE INDEX CONCURRENTLY cannot run inside a transaction.
CREATE INDEX IF NOT EXISTS sessions_org_harness_session_idx
    ON sessions (org_id, harness_session_id);

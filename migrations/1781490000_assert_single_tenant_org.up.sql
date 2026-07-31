-- The API now scopes every read and mutation to the single-tenant sentinel
-- (the nil org UUID), and ingest canonicalizes every write to it. That is
-- only sound if no rows exist under any OTHER org id — data written by an
-- older version under a non-nil org would become unreachable through the
-- API without anything saying so.
--
-- Every deployment we can observe satisfies this already: nothing ever
-- stamped the gateway org header, clients serialize org_id as "", and ingest
-- has canonicalized "" to the sentinel since the raw layer existed. So
-- rather than migrating hypothetical data, this migration PROVES the
-- precondition at deploy time: it fails loudly if any non-sentinel rows
-- exist, leaving the schema untouched and the operator with an explicit
-- decision instead of silently stranded data. (A deployment that trips this
-- has real multi-org rows and needs a deliberate re-home before upgrading;
-- the org_id columns themselves are removed by a follow-up migration.)
DO $$
DECLARE
    sentinel CONSTANT UUID := '00000000-0000-0000-0000-000000000000';
    offenders BIGINT;
    tbl TEXT;
    fixed_tables CONSTANT TEXT[] := ARRAY['sessions', 'raw_turns', 'nodes', 'derive_queue', 'raw_turn_attribution_corrections'];
    versioned RECORD;
BEGIN
    FOREACH tbl IN ARRAY fixed_tables LOOP
        IF to_regclass(tbl) IS NOT NULL THEN
            EXECUTE format(
                'SELECT count(*) FROM %I WHERE org_id IS NOT NULL AND org_id <> %L',
                tbl, sentinel) INTO offenders;
            IF offenders > 0 THEN
                RAISE EXCEPTION
                    'single-tenant precondition failed: % rows in %.org_id are not the nil sentinel — these rows would be unreachable through the sentinel-scoped API; re-home or export them before upgrading',
                    offenders, tbl;
            END IF;
        END IF;
    END LOOP;

    -- The span projection lives in date-suffixed physical tables registered
    -- in derived_projection_schemas; check every registered generation.
    IF to_regclass('derived_projection_schemas') IS NOT NULL THEN
        FOR versioned IN SELECT spans_table, span_turns_table, span_links_table FROM derived_projection_schemas LOOP
            FOREACH tbl IN ARRAY ARRAY[versioned.spans_table, versioned.span_turns_table, versioned.span_links_table] LOOP
                IF to_regclass(tbl) IS NOT NULL THEN
                    EXECUTE format(
                        'SELECT count(*) FROM %I WHERE org_id IS NOT NULL AND org_id <> %L',
                        tbl, sentinel) INTO offenders;
                    IF offenders > 0 THEN
                        RAISE EXCEPTION
                            'single-tenant precondition failed: % rows in %.org_id are not the nil sentinel — re-home or export them before upgrading',
                            offenders, tbl;
                    END IF;
                END IF;
            END LOOP;
        END LOOP;
    END IF;
END $$;

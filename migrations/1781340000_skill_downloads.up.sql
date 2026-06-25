-- download_count is a real usage signal: how many times a skill's SKILL.md has
-- been downloaded (the only concrete "use" action we have today). Unlike the
-- earlier faked metrics, this is incremented on an actual user action, so it
-- can back a "most downloaded" sort honestly.
ALTER TABLE skills
    ADD COLUMN IF NOT EXISTS download_count BIGINT NOT NULL DEFAULT 0;

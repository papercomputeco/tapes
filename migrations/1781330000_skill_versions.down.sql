DROP TABLE IF EXISTS skill_versions;

ALTER TABLE skills
    DROP COLUMN IF EXISTS author_subject;

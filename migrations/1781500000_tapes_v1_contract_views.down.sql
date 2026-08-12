-- Withdraw the v1 read contract. CASCADE is safe here: the schema holds only
-- the four contract views, and any GRANTs on them (a deployment's, not
-- core's) fall away with the views themselves.

DROP SCHEMA IF EXISTS tapes_v1 CASCADE;

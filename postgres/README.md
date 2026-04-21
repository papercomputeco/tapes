# CloudnativePG + `pg_duckdb`

`postgres/` contains the custom Postgres image used by `tapes`.

It layers `pg_duckdb` onto the CloudNativePG Postgres image and initializes:

- `vector`
- `pg_duckdb`

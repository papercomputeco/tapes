-- name: HasNode :one
SELECT EXISTS(
    SELECT 1 FROM nodes WHERE hash = $1
);

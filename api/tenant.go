package api

// singleTenantOrgID is the org every row in this deployment belongs to.
//
// A tapes deployment serves exactly one tenant: it runs in that tenant's
// namespace against that tenant's own database, and the gateway in front of
// it admits only tokens whose org_id claim matches that tenant's WorkOS
// organization. There is nothing for a read to disambiguate, so reads scope
// to the same sentinel the write path already stores.
//
// This replaces the X-Tapes-Org-Id request header, which let a caller name
// its own tenant on every read with nothing verifying the claim — the header
// was trusted the same way the ingest envelope's org_id is, which is to say
// not at all. Anyone who could reach the read API could read any org by
// asserting it. Tenancy is now settled before a request reaches this process,
// by something positioned to check it.
//
// The constant is threaded through the handlers rather than pushed down into
// the storage layer on purpose. The org_id columns and their composite
// primary keys still exist; keeping the value at the handler boundary leaves
// exactly one seam to delete when they go, instead of scattering the
// assumption across the query layer.
const singleTenantOrgID = "00000000-0000-0000-0000-000000000000"

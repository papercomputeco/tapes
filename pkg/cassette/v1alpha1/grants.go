package v1alpha1

import (
	"sort"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// GrantPlan derives the cassette's role, owned schema, readable contract views,
// and declared owned tables.
func (m *Manifest) GrantPlan() cassette.GrantPlan {
	if m == nil {
		return cassette.GrantPlan{}
	}

	name := m.Cassette.Name
	plan := cassette.GrantPlan{
		Role:      name.RoleName(),
		Schema:    name.SchemaName(),
		OwnSchema: true,
		Selects:   make([]string, 0, len(m.Depends.Views)),
		Tables:    make([]string, 0, len(m.Tables)),
	}
	contractSchema := "tapes_" + string(m.Depends.Core)
	for _, view := range m.Depends.Views {
		plan.Selects = append(plan.Selects, contractSchema+"."+view)
	}
	// depends.published views are already schema-qualified: they name another
	// cassette's published schema, not the tapes contract schema.
	plan.Selects = append(plan.Selects, m.Depends.Published...)
	for _, table := range m.Tables {
		plan.Tables = append(plan.Tables, table.Name)
	}
	// The views this cassette publishes are the reverse grant: core's read
	// role needs SELECT on them for claimed filters to execute. Declaration
	// only, like everything else in the plan — the deployment renders it.
	if m.Publishes != nil && len(m.Publishes.Views) > 0 {
		plan.CoreSelects = append([]string(nil), m.Publishes.Views...)
		sort.Strings(plan.CoreSelects)
	}
	sort.Strings(plan.Selects)
	sort.Strings(plan.Tables)

	return plan
}

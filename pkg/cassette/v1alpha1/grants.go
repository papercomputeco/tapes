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
	for _, table := range m.Tables {
		plan.Tables = append(plan.Tables, table.Name)
	}
	sort.Strings(plan.Selects)
	sort.Strings(plan.Tables)

	return plan
}

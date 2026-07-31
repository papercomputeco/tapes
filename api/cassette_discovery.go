package api

import (
	"sort"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Discovery is the document served at GET /v1/cassettes.
//
// It publishes what each cassette *is* — never what it is configured to. Core
// holds no configuration values—the deployment supplies them directly to the
// cassette—so there is nothing here to leak.
type Discovery struct {
	ContractVersion string                     `json:"contract_version"`
	Cassettes       []DiscoveryEntry           `json:"cassettes"`
	Problems        []cassetterunner.Rejection `json:"problems"`
}

// DiscoveryEntry describes one served cassette.
//
// The OpenAPI document is referenced, not inlined. A single spec runs to tens
// of kilobytes and clients poll discovery; inlining five of them turns every
// client boot into a megabyte of mostly-unchanged bytes. The digest is enough
// to know whether the fetch is worth making.
type DiscoveryEntry struct {
	Name           string             `json:"name"`
	Version        string             `json:"version,omitempty"`
	DisplayName    string             `json:"display_name,omitempty"`
	Description    string             `json:"description,omitempty"`
	RoutePrefix    string             `json:"route_prefix"`
	Depends        *DiscoveryDepends  `json:"depends,omitempty"`
	Tables         []string           `json:"tables"`
	Config         []DiscoverySetting `json:"config"`
	OpenAPIPath    string             `json:"openapi_path"`
	OpenAPIStatus  tapesoapi.Status   `json:"openapi_status"`
	ManifestDigest string             `json:"manifest_digest"`
}

// DiscoveryDepends is a cassette's declared dependency on core.
type DiscoveryDepends struct {
	Core  string   `json:"core"`
	Views []string `json:"views"`
}

// DiscoverySetting is one configuration key as a schema, never as a value.
type DiscoverySetting struct {
	Key         string `json:"key"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Secret      bool   `json:"secret"`
	Default     any    `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// cassetteSpecPath is where a cassette's cached OpenAPI document is served.
func cassetteSpecPath(name cassette.Name) string {
	return "/v1/cassettes/" + string(name) + "/openapi.json"
}

// Discovery builds the discovery document. status reports how current core's
// cached spec for a cassette is; a nil status reports every cassette missing,
// which is the honest answer before anything has been fetched.
//
// Everything here is ordered — cassettes by name, settings by declaration order
// which the manifest itself fixes — because a client that diffs digests must
// see a change only when something actually changed.
func buildCassetteDiscovery(
	registry *cassetterunner.Registry,
	contractVersion string,
	status func(cassette.Name) tapesoapi.Status,
) Discovery {
	if status == nil {
		status = func(cassette.Name) tapesoapi.Status { return tapesoapi.Missing }
	}

	instances := registry.Instances()
	document := Discovery{
		ContractVersion: contractVersion,
		Cassettes:       make([]DiscoveryEntry, 0, len(instances)),
		Problems:        registry.Rejections(),
	}
	for _, instance := range instances {
		document.Cassettes = append(document.Cassettes, discoveryEntry(instance, status(instance.Name)))
	}
	sort.Slice(document.Problems, func(i, j int) bool {
		return document.Problems[i].Subject < document.Problems[j].Subject
	})

	return document
}

// discoveryEntry projects one instance. Fields beyond the schema-independent
// contract come from a type switch on the manifest version. The API is the
// version-aware edge that turns a portable manifest into its discovery wire
// representation.
func discoveryEntry(instance *cassetterunner.Instance, status tapesoapi.Status) DiscoveryEntry {
	entry := DiscoveryEntry{
		Name:           string(instance.Name),
		RoutePrefix:    instance.Prefix(),
		Tables:         []string{},
		Config:         []DiscoverySetting{},
		OpenAPIPath:    cassetteSpecPath(instance.Name),
		OpenAPIStatus:  status,
		ManifestDigest: string(instance.Digest),
	}
	manifest, ok := instance.Manifest.(*v1alpha1.Manifest)
	if !ok {
		return entry
	}

	entry.Version = string(manifest.Cassette.Version)
	entry.DisplayName = manifest.Cassette.DisplayName
	entry.Description = manifest.Cassette.Description
	entry.Depends = &DiscoveryDepends{
		Core:  string(manifest.Depends.Core),
		Views: manifest.Depends.Views,
	}
	if entry.Depends.Views == nil {
		entry.Depends.Views = []string{}
	}

	// Tables are published schema-qualified. The manifest declares them bare
	// because they are relative to the cassette's own schema, but a client
	// reading discovery is looking at the whole database and needs the name it
	// would actually write in a query.
	plan := manifest.GrantPlan()
	for _, table := range plan.Tables {
		entry.Tables = append(entry.Tables, plan.Schema+"."+table)
	}

	for index := range manifest.Config {
		setting := manifest.Config[index]
		published := DiscoverySetting{
			Key:         setting.Key,
			Type:        string(setting.Type),
			Required:    setting.Required,
			Secret:      setting.Secret,
			Description: setting.Description,
		}
		// A secret's default is withheld even though a manifest default is by
		// definition not a real credential: publishing one trains clients to
		// treat this field as a place credentials can appear.
		if !setting.Secret {
			published.Default = setting.Default
		}
		entry.Config = append(entry.Config, published)
	}

	return entry
}

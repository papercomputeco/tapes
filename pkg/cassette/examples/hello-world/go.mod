module github.com/papercomputeco/tapes/pkg/cassette/examples/hello-world

go 1.26.1

require github.com/jackc/pgx/v5 v5.8.0

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)

// The example uses the same OpenAPI library core does, so the document a
// cassette publishes and the document core admits are built by one
// implementation. It is a separate module, so the dependency is a local
// replace rather than a version.
require github.com/papercomputeco/tapes v0.0.0

replace github.com/papercomputeco/tapes => ../../../..

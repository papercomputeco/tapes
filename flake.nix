{
  description = "Tapes - Development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    dagger.url = "github:dagger/nix";
    dagger.inputs.nixpkgs.follows = "nixpkgs";
    paper-skills.url = "github:papercomputeco/skills";
    paper-skills.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, dagger, paper-skills }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        skills = paper-skills.lib;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            # Go toolchain
            pkgs.go_1_26
            pkgs.gotools
            pkgs.go-swag
            pkgs.sqlc

            # Build tools
            pkgs.gnumake
            dagger.packages.${system}.dagger

            # Version control
            pkgs.git

            pkgs.hurl
          ];

          # Enable Go's experimental JSON v2 implementation
          GOEXPERIMENT = "jsonv2";

          # Point the DB-backed suites at the docker-compose Postgres, so a
          # plain `go test ./...` in this shell works once that service is up
          # (`make test-db-up`) instead of failing on an unset variable.
          #
          # The address matches docker-compose.yaml's postgres service. It is
          # only an address: the pipeline sets this variable to its own service
          # binding, so exporting it here cannot affect CI. What both must
          # agree on is the IMAGE, which is why compose and .dagger/postgres.go
          # pin the same tag and `make test-db-up` checks they still do.
          TEST_POSTGRES_DSN = "postgres://tapes:tapes@127.0.0.1:5432/tapes?sslmode=disable";

          shellHook = 
            (skills.mkSkillsHook {
              skills = [ "dagger-check" ];
            })
            +
          ''
            echo "Tapes development environment"
            echo ""
            echo "Go version: $(go version)"
            echo ""
            echo "Available make targets:"
            make help
          '';
        };
      }
    );
}

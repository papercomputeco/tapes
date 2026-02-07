# Contributing

## Nix flake and `direnv`

We encourage contributors to utilize the Nix flake which carries all development dependencies
and has automatic `direnv` support:
to enter the nix development shell, run `nix develop`.
Allow automatic environment loading with `direnv` via `direnv allow`.

## Local demo data

Seed demo sessions for the deck UI without touching real data:

```bash
tapes deck -m
tapes deck --demo --sqlite ./tapes.demo.sqlite
```

To reset the demo database:

```bash
tapes deck -m -f
```

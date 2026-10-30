# pool protocol v1

Shared protocol contract between the llama.cpp-pool router (C++) and the pool-coordinator (Go): message catalog (`catalog.md`), one JSON Schema (draft 2020-12) per message type + envelope (`schemas/`), golden examples (`examples/`). Design of record: `../docs/DECISIONS.md`.

Run the oracle: `python3 protocol/validate.py` (needs `jsonschema`; if missing: `python3 -m venv .venv-protocol && .venv-protocol/bin/pip install jsonschema`), then the Go mirror test: `cd go/poolproto && go test ./...`.

Both sides mirror this directory: the C++ router parses/emits these envelopes on the /pool link; Go uses `go/poolproto` (typed structs, stdlib only). Keep catalog, schemas, examples, and go/poolproto in lockstep: change one, update the others, re-run both oracles.

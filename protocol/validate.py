#!/usr/bin/env python3
"""Structural oracle for the pool protocol v1 contract.

Checks (all must pass; exit code 0 only if so):
  1. every examples/*.json validates against its schema, and the envelope
     "type" matches the message type the filename documents;
  2. catalog.md names all 11 envelope message types and the 2 endpoints
     (POST /pool/jobs, body pull GET /pool/jobs/{job_id}/body);
  3. byte-stability: re-serializing each example (sorted keys, compact
     separators) twice yields identical bytes;
  4. no example file contains a string longer than 250 chars
     (anti-memory sanity: catches accidental prompt text).

Usage:
  python3 validate.py

Requires the `jsonschema` package (Python). If it is not importable, create
a venv and re-run:
  python3 -m venv .venv-protocol
  .venv-protocol/bin/pip install jsonschema
  .venv-protocol/bin/python protocol/validate.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCHEMAS = ROOT / "schemas"
EXAMPLES = ROOT / "examples"
CATALOG = ROOT / "catalog.md"

# Closed set of protocol v1 envelope types (must match schemas/event.json).
TYPES = [
    "handshake",
    "capacity",
    "work_offer",
    "offer_accept",
    "offer_reject",
    "body_abort",
    "completion",
    "error",
    "affinity_report",
    "exclusion_update",
    "eviction_notice",
]
# Endpoints the catalog must name (besides the 11 types).
ENDPOINTS = ["/pool/jobs", "/pool/jobs/{job_id}/body"]
# Transport endpoints the catalog must also document.
TRANSPORT = ["/pool/events", "/pool/subscribe"]

MAX_STRING_LEN = 250

failures: list[str] = []
n_checks = 0


def ok(name: str) -> None:
    global n_checks
    n_checks += 1
    print(f"  ok   {name}")


def fail(name: str, detail: str) -> None:
    global n_checks
    n_checks += 1
    failures.append(f"{name}: {detail}")
    print(f"  FAIL {name}: {detail}")


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def expected_type(stem: str) -> str | None:
    """Map an example filename stem to the message type it documents.

    "<type>.json" documents <type>; "<type>_<variant>.json" documents <type>
    (no v1 type name is a prefix of another, so this is unambiguous).
    """
    if stem in TYPES:
        return stem
    if stem == "job_intake":
        return "job_intake"
    prefixes = [t for t in TYPES if stem.startswith(t + "_")]
    if len(prefixes) == 1:
        return prefixes[0]
    return None


def canonical_bytes(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def walk_strings(obj):
    """Yield every string in a parsed JSON document (keys and values)."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk_strings(v)


def main() -> int:
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        print("ERROR: the 'jsonschema' package is required.")
        print("  python3 -m venv .venv-protocol && .venv-protocol/bin/pip install jsonschema")
        print("  .venv-protocol/bin/python protocol/validate.py")
        return 2

    if not CATALOG.is_file():
        print(f"ERROR: missing {CATALOG}")
        return 1

    # -- Load schemas and sanity-check that they are valid draft 2020-12. ----
    print("== schemas ==")
    schemas: dict[str, dict] = {}
    for name in ["event", "job_intake", *TYPES]:
        path = SCHEMAS / f"{name}.json"
        if not path.is_file():
            fail(f"schema {name}.json", "file missing")
            continue
        try:
            schemas[name] = load_json(path)
            Draft202012Validator.check_schema(schemas[name])
            ok(f"schema {name}.json is valid draft 2020-12")
        except Exception as e:  # noqa: BLE001 - report any schema defect
            fail(f"schema {name}.json", str(e).splitlines()[0])

    # The event envelope must list exactly the 11 closed types.
    if "event" in schemas:
        enum = schemas["event"]["properties"]["type"].get("enum", [])
        if sorted(enum) == sorted(TYPES):
            ok("event.json type enum matches the closed v1 set")
        else:
            fail("event.json type enum", f"{sorted(enum)} != {sorted(TYPES)}")

    examples = sorted(EXAMPLES.glob("*.json"))
    if not examples:
        print(f"ERROR: no example files under {EXAMPLES}")
        return 1

    # -- Check 1: every example validates against its schema. ----------------
    print("== examples validate ==")
    for path in examples:
        stem = path.stem
        etype = expected_type(stem)
        if etype is None:
            fail(f"example {path.name}", f"no message type matches stem {stem!r}")
            continue
        try:
            doc = load_json(path)
        except Exception as e:  # noqa: BLE001
            fail(f"example {path.name}", f"not valid JSON: {e}")
            continue

        errors = []
        if etype == "job_intake":
            # Job intake metadata is a plain JSON body, not an envelope.
            if etype not in schemas:
                fail(f"example {path.name}", "schemas/job_intake.json missing")
                continue
            errors = [
                f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}"
                for e in Draft202012Validator(schemas[etype]).iter_errors(doc)
            ]
        else:
            if etype not in schemas:
                fail(f"example {path.name}", f"schemas/{etype}.json missing")
                continue
            errors = [
                f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}"
                for e in Draft202012Validator(schemas[etype]).iter_errors(doc)
            ]
            # Envelope type must match the file's expected type (the per-type
            # schema enforces this via const; state it explicitly as well).
            if not isinstance(doc, dict) or doc.get("type") != etype:
                errors.append(f"envelope type {doc.get('type')!r} != {etype!r}")
            # Must also validate against the generic event envelope.
            if "event" in schemas:
                errors += [
                    f"event.json: {'/'.join(map(str, e.path)) or '<root>'}: {e.message}"
                    for e in Draft202012Validator(schemas["event"]).iter_errors(doc)
                ]
        if errors:
            fail(f"example {path.name}", "; ".join(errors[:4]))
        else:
            ok(f"example {path.name} validates against schemas/{etype}.json")

    # -- Check 2: catalog names all 11 types + the 2 endpoints. -------------
    print("== catalog names ==")
    catalog_text = CATALOG.read_text(encoding="utf-8")
    for name in [*TYPES, *ENDPOINTS, *TRANSPORT]:
        if name in catalog_text:
            ok(f"catalog names {name}")
        else:
            fail(f"catalog names {name}", "not found in catalog.md")

    # -- Check 3: byte-stability of example serialization. -------------------
    print("== byte-stability ==")
    for path in examples:
        try:
            doc = load_json(path)
        except Exception as e:  # noqa: BLE001
            fail(f"byte-stability {path.name}", f"not valid JSON: {e}")
            continue
        s1 = canonical_bytes(doc)
        s2 = canonical_bytes(json.loads(s1))
        if s1 == s2:
            ok(f"byte-stability {path.name}")
        else:
            fail(f"byte-stability {path.name}", "re-serialization is not idempotent")

    # -- Check 4: no string longer than 250 chars (anti-memory). --------------
    print("== anti-memory (string length) ==")
    for path in examples:
        try:
            doc = load_json(path)
        except Exception as e:  # noqa: BLE001
            fail(f"string-length {path.name}", f"not valid JSON: {e}")
            continue
        too_long = [(i, len(s)) for i, s in enumerate(walk_strings(doc)) if len(s) > MAX_STRING_LEN]
        if too_long:
            fail(
                f"string-length {path.name}",
                f"{len(too_long)} string(s) longer than {MAX_STRING_LEN} chars "
                f"(max len {max(l for _, l in too_long)}) - possible prompt/response content",
            )
        else:
            ok(f"string-length {path.name} (no string > {MAX_STRING_LEN} chars)")

    # -- Summary. -------------------------------------------------------------
    print()
    if failures:
        print(f"FAILED: {len(failures)}/{n_checks} checks failed")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"ALL CHECKS PASSED ({n_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

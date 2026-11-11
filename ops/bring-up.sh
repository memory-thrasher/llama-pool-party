#!/usr/bin/env bash
# bring-up.sh - single cold start: models + pool router + coordinator.
#
# REQUIREMENT (REQ-3, owner 2026-11-08): one script that brings up the
# whole pool stack from a COLD host - nothing is assumed to be running
# first (in particular NOT the stock production server). This is the
# member-side bring-up for the group deployment: each member runs this
# on their machine; the routers then connect OUTBOUND to the cloud
# coordinator. For solo use this also runs a local coordinator.
#
# Thin wrapper over run-demo.sh (which carries all the machinery:
# scratch dir, symlinks, no-download, augmented config, tags, smoke).
#
#   ./bring-up.sh                  fleet preset, owner's class mapping,
#                                  host bind, no prod required
#   ./bring-up.sh --smoke          + scripted end-to-end check
#   ./bring-up.sh --preset X       a different fleet preset
#   ./bring-up.sh --port 8123 ...  any run-demo.sh option passes through
#
# Class mapping (owner 2026-11-08): Qwen-Drive* -> pool-image-text-xs
# (it is a VLM); everything else in the fleet -> pool-text-md.
set -euo pipefail
OPS="$(cd "$(dirname "$0")" && pwd)"
PRESET="$OPS/swap-fleet.ini"
HOST="0.0.0.0"
EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --preset) PRESET="$2"; shift 2 ;;
    --host)   HOST="$2"; shift 2 ;;
    *)        EXTRA+=("$1"); shift ;;
  esac
done
[ -f "$PRESET" ] || { echo "[bring-up] preset not found: $PRESET" >&2; exit 1; }

# no-download guard + alias list in one pass, same logic as the swap
# wrapper: section name = <models-dir>/<alias>.gguf must exist on disk.
MODELS_DIR="${MODELS_DIR:-/mnt/data/ai_models}"
ALIASES=$(python3 - "$PRESET" "$MODELS_DIR" <<'PY'
import os, sys
preset, mdir = sys.argv[1], sys.argv[2]
aliases = [l.strip()[1:-1].strip() for l in open(preset)
           if l.strip().startswith("[") and l.strip().endswith("]")]
for a in aliases:
    if not os.path.isfile(os.path.join(mdir, a + ".gguf")):
        print("missing model file: " + a, file=sys.stderr)
        sys.exit(1)
print("\n".join(aliases))
PY
) || { echo "[bring-up] aborting: fix the preset or point it at existing files" >&2; exit 1; }

# class tags: owner's mapping (Qwen-Drive* = image-capable)
TAGS=()
for alias in $ALIASES; do
  case "$alias" in
    Qwen-Drive*) TAGS+=("$alias=pool-image-text-xs") ;;
    *)           TAGS+=("$alias=pool-text-md") ;;
  esac
done
for t in "${TAGS[@]}"; do EXTRA+=("--tag" "$t"); done

echo "[bring-up] preset=$PRESET host=$HOST tags: ${TAGS[*]}"
exec "$OPS/run-demo.sh" --router-config "$PRESET" --host "$HOST" "${EXTRA[@]}"

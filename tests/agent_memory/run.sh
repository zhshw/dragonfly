#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ams_root="${AMS_ROOT:-$repo_root/../agent-memory-server/V0}"
uv_bin="${UV_BIN:-uv}"

"$uv_bin" sync --project "$ams_root" --frozen --group dev
source "$ams_root/.venv/bin/activate"
export AMS_ROOT="$ams_root"
export DRAGONFLY_PATH="${DRAGONFLY_PATH:-$repo_root/build-dbg/dragonfly}"
export LITELLM_LOCAL_MODEL_COST_MAP=True
export HF_HUB_OFFLINE=1
python "$repo_root/tools/agent_memory/audit_docket.py"
python -m pytest -c "$repo_root/tests/agent_memory/pytest.ini" "$repo_root/tests/agent_memory" "$@"

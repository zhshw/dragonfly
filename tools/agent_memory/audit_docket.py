"""Check the reviewed Docket script inventory before allowing any Lua SHA."""

import ast
import hashlib
import json
from importlib.metadata import version
from pathlib import Path

import docket

from agent_memory_server.docket_worker import (
    BOUNDED_SCHEDULER_SHA,
    validate_docket_scheduler,
)
from agent_memory_server.bounded_docket import (
    BOUNDED_SCHEDULE_SHA,
    BOUNDED_CLAIM_SHA,
    BOUNDED_CANCEL_SHA,
)


MANIFEST = Path(__file__).with_name("docket-scripts.json")


def inventory():
    base = Path(docket.__file__).parent
    scripts = {}
    for path in sorted(base.rglob("*.py")):
        tree = ast.parse(path.read_text())
        constants = {}
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "register_script"
            ):
                continue
            argument = node.args[0]
            script = (
                constants[argument.id]
                if isinstance(argument, ast.Name)
                else ast.literal_eval(argument)
            )
            sha = hashlib.sha1(script.encode()).hexdigest()
            scripts[sha] = {"file": str(path.relative_to(base)), "line": node.lineno}
    return scripts


def main():
    reviewed = json.loads(MANIFEST.read_text())
    assert version("pydocket") == reviewed["version"], "Docket version changed; re-audit required"
    actual = inventory()
    expected = {script["sha"]: script for script in reviewed["scripts"]}
    assert set(actual) == set(expected), "Lua inventory changed; re-audit required"
    for sha, source in actual.items():
        assert source["file"] == expected[sha]["file"]
    validate_docket_scheduler()
    assert reviewed["ams_scripts"] == [
        {
            "module": "agent_memory_server.docket_worker",
            "constant": "BOUNDED_SCHEDULER_SCRIPT",
            "sha": BOUNDED_SCHEDULER_SHA,
            "purpose": "atomic due scheduler with LIMIT 64 and selected-ID ZREM",
        },
        {
            "module": "agent_memory_server.bounded_docket",
            "constant": "BOUNDED_SCHEDULE_SCRIPT",
            "sha": BOUNDED_SCHEDULE_SHA,
            "purpose": "atomic ZCARD plus XLEN admission with six declared keys",
        },
        {
            "module": "agent_memory_server.bounded_docket",
            "constant": "BOUNDED_CLAIM_SCRIPT",
            "sha": BOUNDED_CLAIM_SHA,
            "purpose": "claim retains stream ID for atomic active cancellation",
        },
        {
            "module": "agent_memory_server.bounded_docket",
            "constant": "BOUNDED_CANCEL_SCRIPT",
            "sha": BOUNDED_CANCEL_SHA,
            "purpose": "atomic cancel XACK and XDEL with six declared keys",
        },
    ], "AMS worker Lua changed; re-audit required"
    assert reviewed["allowed_shas"] == [reviewed["allowed_sha"], BOUNDED_SCHEDULER_SHA]
    print(
        json.dumps(
            {
                "version": reviewed["version"],
                "scripts": actual,
                "ams_scripts": reviewed["ams_scripts"],
                "dragonfly_flags": [
                    f"--lua_undeclared_keys_shas={','.join(reviewed['allowed_shas'])}"
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

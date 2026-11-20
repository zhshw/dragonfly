"""Regenerate Dragonfly metadata record codecs using the KRaft schema generator."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "protocol"))
from generate import generate


if __name__ == "__main__":
    generate(
        ROOT / "schemas",
        ["RedisNodeRecord", "DataGroupRecord", "GroupLeaderRecord", "SlotAssignmentRecord"],
        "slot_records",
        source="Dragonfly cluster extension schemas",
    )

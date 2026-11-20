"""Regenerate Dragonfly metadata record codecs using the KRaft schema generator."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "protocol"))
from generate import generate


if __name__ == "__main__":
    generate(
        ROOT / "schemas",
        [
            "RedisNodeRecord",
            "DataGroupRecord",
            "GroupLeaderRecord",
            "SlotAssignmentRecord",
            "SlotMigrationRecord",
        ],
        "slot_records",
        source="Dragonfly cluster extension schemas",
    )
    generate(
        ROOT / "schemas",
        ["SlotMigrationRequest", "SlotMigrationResponse"],
        "slot_migration",
        source="Dragonfly cluster extension schemas",
    )
    generate(
        ROOT / "schemas",
        ["FetchSlotImageRequest", "FetchSlotImageResponse"],
        "fetch_slot_image",
        source="Dragonfly cluster extension schemas",
    )
    generate(
        ROOT / "schemas",
        ["SlotTransferRecord"],
        "slot_transfer_record",
        source="Dragonfly cluster extension schemas",
    )

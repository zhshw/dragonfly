"""Install mount ordering and a boot service for this isolated observability stack."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import subprocess


def install(device, storage, disk_image, compose, env_file):
    if os.geteuid() != 0:
        raise PermissionError("Run setup_host.sh with sudo")
    uuid = subprocess.check_output(
        ["blkid", "-s", "UUID", "-o", "value", device], text=True
    ).strip()
    entries = {
        "/data": f"UUID={uuid} /data ntfs3 uid=1000,gid=1000,umask=022,nofail,x-systemd.device-timeout=10s 0 0",
        storage: f"{disk_image} {storage} ext4 loop,nofail,x-systemd.requires-mounts-for=/data 0 0",
    }
    fstab = Path("/etc/fstab")
    original = fstab.read_text()
    for line in original.splitlines():
        fields = line.split()
        if not fields or fields[0].startswith("#") or len(fields) < 2:
            continue
        if fields[1] in entries:
            if fields[:3] != entries[fields[1]].split()[:3]:
                raise ValueError(f"Conflicting existing fstab mount: {fields[1]}")
            del entries[fields[1]]
    if entries:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(fstab, f"/etc/fstab.dragonfly-observability-{stamp}.bak")
        with fstab.open("a") as target:
            target.write(
                "\n# Dragonfly observability: existing NTFS volume and separate ext4 image.\n"
            )
            target.write("\n".join(entries.values()) + "\n")
    command = f"/usr/bin/docker compose --env-file {env_file} -f {compose}"
    unit = f"""[Unit]
Description=Dragonfly Langfuse and profile artifacts
Requires=docker.service
After=docker.service
RequiresMountsFor=/data {storage}
ConditionPathIsMountPoint={storage}

[Service]
Type=oneshot
RemainAfterExit=yes
TimeoutStartSec=900
ExecStart={command} up -d --wait --wait-timeout 180
ExecStop={command} stop

[Install]
WantedBy=multi-user.target
"""
    Path("/etc/systemd/system/dragonfly-observability.service").write_text(unit)
    print("Installed fstab entries and dragonfly-observability.service")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("device", "storage", "disk-image", "compose", "env-file"):
        parser.add_argument("--" + name, required=True)
    install(**vars(parser.parse_args()))

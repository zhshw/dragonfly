#!/usr/bin/env bash
# Mount the existing Data volume and deploy an isolated Langfuse stack.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run this script with sudo on 172.31.23.2." >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
task_user=zhangshenwang
task_uid=$(id -u "$task_user")
task_gid=$(id -g "$task_user")
device=/dev/sda1
storage=/data/dragonfly-observability
disk_image=/data/dragonfly-observability.ext4

[[ $(blkid -s TYPE -o value "$device") == ntfs ]] || {
  echo "Expected the existing NTFS Data partition at $device; stopping." >&2
  exit 1
}
mkdir -p /data
if mountpoint -q /data; then
  [[ $(readlink -f "$(findmnt -rn -o SOURCE --mountpoint /data)") == $device ]] || {
    echo "/data is mounted from another device; stopping." >&2
    exit 1
  }
else
  existing_mount=$(findmnt -rn -S "$device" -o TARGET | head -n 1)
  if [[ -n $existing_mount ]]; then
    # Preserve a desktop mount that may still have open files.
    mount --bind "$existing_mount" /data
  else
    mount -t ntfs3 -o "uid=$task_uid,gid=$task_gid,umask=022" "$device" /data
  fi
fi

# The original NTFS filesystem is preserved. A new 64 GiB ext4 image supplies database
# ownership, locking and directory-sync semantics; no existing partition is formatted.
# NTFS mounts with prealloc can allocate the full image even though truncate is used.
if [[ ! -e $disk_image ]]; then
  (set -o noclobber; : > "$disk_image")
  truncate -s 64G "$disk_image"
  mkfs.ext4 -q -E nodiscard "$disk_image"
else
  [[ -f $disk_image && ! -L $disk_image && $(blkid -s TYPE -o value "$disk_image") == ext4 ]] || {
    echo "Existing storage image is not a regular ext4 image; stopping." >&2
    exit 1
  }
fi
mkdir -p "$storage"
if ! mountpoint -q "$storage"; then
  mount -o loop "$disk_image" "$storage"
fi
[[ $(findmnt -rn -o FSTYPE --mountpoint "$storage") == ext4 ]] || exit 1
mounted_device=$(findmnt -rn -o SOURCE --mountpoint "$storage")
[[ $(losetup --noheadings --output BACK-FILE "$mounted_device") == "$disk_image" ]] || {
  echo "Storage mount does not use the expected image; stopping." >&2
  exit 1
}

install -d -m 0750 -o "$task_uid" -g "$task_gid" "$storage/config"
install -d -m 0755 "$storage/state" "$storage/state/postgres" "$storage/state/redis"
install -d -m 0755 -o 101 -g 101 "$storage/state/clickhouse" "$storage/state/clickhouse-logs"
install -d -m 0755 -o "$task_uid" -g "$task_gid" "$storage/state/minio"

# Artifacts use the same root as the bind-mounted databases.
install -d -m 0755 -o "$task_uid" -g "$task_gid" "$storage/state/artifacts"
env_file=$storage/config/langfuse.env
if [[ ! -e $env_file ]]; then
  runuser -u "$task_user" -- python3 "$script_dir/init_env.py" \
    --env-file "$env_file" --storage-root "$storage/state"
fi
docker compose --env-file "$env_file" -f "$script_dir/compose.yaml" config -q
python3 "$script_dir/persist_host.py" --device "$device" --storage "$storage" \
  --disk-image "$disk_image" --compose "$script_dir/compose.yaml" --env-file "$env_file"
systemctl daemon-reload
systemctl enable dragonfly-observability.service
systemctl start dragonfly-observability.service
findmnt --mountpoint /data
findmnt --mountpoint "$storage"
echo "Langfuse: http://172.31.23.2:3007 ; profile artifacts: http://172.31.23.2:3137"
echo "Credentials: $env_file (not printed)"

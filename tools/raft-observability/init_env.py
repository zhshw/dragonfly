"""Create local Langfuse credentials without printing or replacing existing secrets."""

import argparse
import os
from pathlib import Path
import secrets
import uuid


def create(path, storage):
    values = {
        "LANGFUSE_STORAGE_ROOT": str(storage.resolve()),
        "OBSERVABILITY_BIND": "0.0.0.0",
        "LANGFUSE_PUBLIC_URL": "http://172.31.23.2:3007",
        "LANGFUSE_ADMIN_EMAIL": "admin@local.test",
        "LANGFUSE_PUBLIC_KEY": "pk-lf-" + str(uuid.uuid4()),
        "LANGFUSE_SECRET_KEY": "sk-lf-" + secrets.token_hex(32),
    }
    for name in (
        "POSTGRES_PASSWORD",
        "CLICKHOUSE_PASSWORD",
        "REDIS_PASSWORD",
        "MINIO_PASSWORD",
        "LANGFUSE_SALT",
        "LANGFUSE_ENCRYPTION_KEY",
        "NEXTAUTH_SECRET",
        "LANGFUSE_ADMIN_PASSWORD",
    ):
        values[name] = secrets.token_hex(32)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write("".join(f"{key}={value}\n" for key, value in values.items()))
    print(f"Credentials saved with mode 0600: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--storage-root", type=Path, required=True)
    args = parser.parse_args()
    create(args.env_file, args.storage_root)

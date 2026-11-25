#!/usr/bin/env bash
# Run once under Compose before starting the experiment executor.
set -euo pipefail
umask 077

db=dragonfly_experiments
dsn=/config/raft-workflow.dsn
owner="${PROFILE_UID}:${PROFILE_GID}"
psql=(psql -XqAt -h postgres -U langfuse -d postgres -v ON_ERROR_STOP=1)

# Record a host-routable address for the existing CLI. The container uses Compose DNS.
address=$("${psql[@]}" -c 'SELECT host(inet_server_addr())')
exists=$("${psql[@]}" -c "SELECT count(*) FROM pg_roles WHERE rolname='$db'")
if [[ -f $dsn ]]; then
  password=$(sed -n 's/^.* password=\([a-f0-9]\{64\}\) .*$/\1/p' "$dsn")
  [[ $password =~ ^[a-f0-9]{64}$ ]] || { echo 'Invalid experiment connection file' >&2; exit 1; }
elif [[ $exists == 0 ]]; then
  password=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
else
  echo 'Experiment role exists but its connection file is missing' >&2
  exit 1
fi

# Write the credential before creating the role so a retry can finish interrupted provisioning.
write_dsn() {
  local file=$1 host=$2 tmp
  tmp=$(mktemp "${file}.XXXXXX")
  printf 'host=%s port=5432 dbname=%s user=%s password=%s sslmode=disable\n' \
    "$host" "$db" "$db" "$password" > "$tmp"
  chown "$owner" "$tmp"
  mv -f "$tmp" "$file"
}
write_dsn "$dsn" "$address"
if [[ $exists == 0 ]]; then
  "${psql[@]}" <<SQL
CREATE ROLE $db LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '$password';
SQL
fi
"${psql[@]}" <<SQL
SELECT format('CREATE DATABASE %I OWNER %I', '$db', '$db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='$db') \gexec
REVOKE CONNECT ON DATABASE $db FROM PUBLIC;
SQL
PGPASSWORD="$password" psql -XqAt -h postgres -U "$db" -d "$db" -c 'SELECT 1' >/dev/null

install -d -m 0750 -o "$PROFILE_UID" -g "$PROFILE_GID" /archive /runtime
write_dsn /runtime/raft-workflow.dsn postgres
rm -f /runtime/langfuse.env

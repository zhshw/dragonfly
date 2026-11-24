#!/usr/bin/env bash
# Create an isolated experiment database in the existing observability PostgreSQL.
set -euo pipefail

owner=${SUDO_USER:?run with sudo from the experiment account}
root=${1:-/data/dragonfly-observability}

# Reuse the database connection file and artifact directory from existing deployments.
work="$root/state/raft-workflow"
dsn="$root/config/raft-workflow.dsn"
db=dragonfly_experiments

mapfile -t postgres < <(docker ps \
  --filter 'label=com.docker.compose.project=dragonfly-observability' \
  --filter 'label=com.docker.compose.service=postgres' --format '{{.ID}}')
if (( ${#postgres[@]} != 1 )); then
  echo 'Expected one observability PostgreSQL container' >&2
  exit 1
fi
psql() {
  docker exec -i "${postgres[0]}" psql -XqAt -U langfuse -d postgres -v ON_ERROR_STOP=1 "$@"
}

address=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${postgres[0]}")
if [[ ! $address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo 'PostgreSQL container must have a single IPv4 network address' >&2
  exit 1
fi
install -d -m 0750 -o "$owner" -g "$(id -gn "$owner")" "$work"

if [[ -f $dsn ]]; then
  password=$(sed -n 's/^.* password=\([a-f0-9]\{64\}\) .*$/\1/p' "$dsn")
  [[ $password =~ ^[a-f0-9]{64}$ ]]
else
  if [[ $(psql -c "SELECT count(*) FROM pg_roles WHERE rolname='$db'") != 0 ]]; then
    echo 'Experiment role exists but its connection file is missing' >&2
    exit 1
  fi
  password=$(openssl rand -hex 32)
  # Feed the password through standard input and keep it out of command arguments.
  psql <<SQL
CREATE ROLE $db LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '$password';
SQL
fi
temporary=$(mktemp "$root/config/.profile-XXXXXX")
trap 'rm -f "$temporary"' EXIT
printf 'host=%s port=5432 dbname=%s user=%s password=%s sslmode=disable\n' \
  "$address" "$db" "$db" "$password" > "$temporary"
chown "$owner:$(id -gn "$owner")" "$temporary"
chmod 0600 "$temporary"
mv -f "$temporary" "$dsn"

psql <<SQL
SELECT format('CREATE DATABASE %I OWNER %I', '$db', '$db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='$db') \gexec
REVOKE CONNECT ON DATABASE $db FROM PUBLIC;
SQL
printf 'PostgreSQL database %s ready; experiment directory %s\n' "$db" "$work"

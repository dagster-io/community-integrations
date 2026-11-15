#!/bin/sh
# Optional entrypoint prefix: reach a Postgres database behind Cloudflare Access.
# Set ENTRYPOINT_PREFIX to ["/opt/dagster-cloudflare/with-access-tunnel.sh"] in
# the Worker and these variables in RUN_ENV_JSON:
#   ACCESS_DB_HOSTNAME   Access application hostname of the database, e.g. db.example.com
#   TUNNEL_SERVICE_TOKEN_ID / TUNNEL_SERVICE_TOKEN_SECRET   Access service token
#   ACCESS_DB_LOCAL_PORT local port for Dagster's Postgres host 127.0.0.1 (default 15432)
# The image needs the cloudflared binary, for example:
#   COPY --from=cloudflare/cloudflared:latest /usr/local/bin/cloudflared /usr/local/bin/
#   COPY with-access-tunnel.sh /opt/dagster-cloudflare/with-access-tunnel.sh
set -eu
port="${ACCESS_DB_LOCAL_PORT:-15432}"
py=$(command -v python || command -v python3)
cloudflared access tcp --hostname "$ACCESS_DB_HOSTNAME" --url "127.0.0.1:$port" &
tunnel=$!
trap 'kill "$tunnel" 2>/dev/null || true' EXIT

i=0
until "$py" -c "import socket,sys; socket.create_connection(('127.0.0.1', int(sys.argv[1])), 1)" "$port" 2>/dev/null; do
  i=$((i + 1))
  if [ "$i" -ge 150 ] || ! kill -0 "$tunnel" 2>/dev/null; then
    echo "with-access-tunnel: database tunnel did not start" >&2
    exit 1
  fi
  sleep 0.2
done

# Run the Dagster command as a child and pass SIGTERM on, so a cancel reaches it.
"$@" &
child=$!
trap 'kill -TERM "$child" 2>/dev/null' TERM INT
status=0
wait "$child" || status=$?
# wait returns early when a trap runs; wait again for the real exit status.
if kill -0 "$child" 2>/dev/null; then wait "$child" || status=$?; fi
exit "$status"

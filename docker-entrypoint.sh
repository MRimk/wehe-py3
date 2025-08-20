#!/usr/bin/env bash
set -e

# default flags
RUN_TOPOS=false
RUN_SERVER=false
PASS_ARGS=()

# parse flags once
for arg in "$@"; do
  case "$arg" in
    --topos)
      RUN_TOPOS=true
      ;;
    --run_server)
      RUN_SERVER=true
      ;;
    *)
      PASS_ARGS+=("$arg")
      ;;
  esac
done

# optionally download topologies
if [ "$RUN_TOPOS" = true ]; then
  echo "[entrypoint] downloading topologies"
  ./run_bq.sh
else
  echo "[entrypoint] skipping topologies"
fi

# either start the server or drop to shell
if [ "$RUN_SERVER" = true ]; then
  echo "[entrypoint] starting server"
  exec ./startserver.sh "${PASS_ARGS[@]}"
else
  echo "[entrypoint] dropping to shell"
  exec /bin/bash
fi
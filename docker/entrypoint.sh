#!/bin/sh
set -eu

if [ -z "${SPARK_LOCAL_IP:-}" ]; then
  SPARK_LOCAL_IP="$(hostname -i | awk '{print $1}')"
  export SPARK_LOCAL_IP
fi

exec "$@"

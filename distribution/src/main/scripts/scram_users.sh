#!/bin/sh
#
# Copyright (c) 2012-2024 The original author or authors
#
# Generates the SCRAM-SHA-256 data (salt, iterations, stored key, server key) to store for a user.

unset CDPATH
# Resolve the script path also when launched from a symlink
if [ -L "$0" ]; then
  RL="$(command -v readlink)"
  if [ $? -eq 0 ]; then
    SOURCEPATH="$($RL "$0")"
  else
    echo "You may need to launch this script with a full path instead of a symlink."
    exit 1
  fi
else
  SOURCEPATH="$0"
fi

MOQUETTE_HOME="$(cd "$(dirname "$SOURCEPATH")"/..; pwd)"
export MOQUETTE_HOME

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then
   JAVA="${JAVA_HOME}/bin/java"
else
   JAVA=java
fi

exec "$JAVA" -Djava.awt.headless=true -Dmoquette.path="$MOQUETTE_HOME" -cp "$MOQUETTE_HOME/lib/*" io.moquette.broker.security.scram.ScramCredentialsGenerator "$@"

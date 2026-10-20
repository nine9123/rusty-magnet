#!/bin/sh
# Makes sure the database directory is writable before starting rustymagnet.
#
# A bind mount such as ./data:/data that Docker created itself is owned by
# root, and rustymagnet (running as an unprivileged user) then exits straight
# away with "unable to open database file". When the container starts as root
# (the default), this script hands the directory to PUID:PGID (default
# 1000:1000) and drops privileges to that user. When it is started with
# `user:` it can't change ownership, so it only explains the problem.
set -e

db_dir=$(dirname "${RUSTYMAGNET_DB:-/data/rustymagnet.db}")

if [ "$(id -u)" = 0 ]; then
    uid=${PUID:-1000}
    gid=${PGID:-1000}
    mkdir -p "$db_dir"
    # Only touch ownership when it is actually wrong, so a large existing
    # database isn't chowned on every start.
    if [ "$(stat -c %u:%g "$db_dir")" != "$uid:$gid" ]; then
        echo "entrypoint: changing owner of $db_dir to $uid:$gid" >&2
        chown -R "$uid:$gid" "$db_dir"
    fi
    exec setpriv --reuid="$uid" --regid="$gid" --clear-groups rustymagnet "$@"
fi

if [ ! -w "$db_dir" ]; then
    echo "entrypoint: $db_dir is missing or not writable by uid $(id -u); run" >&2
    echo "  sudo chown -R $(id -u):$(id -g) <host directory mounted at $db_dir>" >&2
    echo "or start the container as root and set PUID/PGID instead of user:" >&2
    exit 1
fi
exec rustymagnet "$@"

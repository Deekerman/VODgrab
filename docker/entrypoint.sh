#!/bin/sh
set -e

# Run VODgrab as PUID:PGID so files in /data and /downloads match the Sonarr and
# Radarr containers. Set PUID=0 to stay root.
PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

if [ "$(id -u)" = "0" ] && [ "$PUID" != "0" ]; then
    mkdir -p "$VODGRAB_DATA"
    chown -R "$PUID:$PGID" "$VODGRAB_DATA"
    # Hand over VODgrab's default download folder (not the rest of the share).
    if [ -d /downloads ] && [ ! -e /downloads/iptv ]; then
        mkdir /downloads/iptv
        chown "$PUID:$PGID" /downloads/iptv
    fi
    exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
        python3 /app/vodgrab.py "$@"
fi

exec python3 /app/vodgrab.py "$@"

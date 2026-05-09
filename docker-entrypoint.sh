#!/bin/sh
# Drop privileges to PUID:PGID before exec'ing fusion. Pattern borrowed from
# linuxserver.io images: NAS users (Unraid, Synology, QNAP, TrueNAS, OMV) all
# have media owned by different UIDs, so a hardcoded UID never works
# universally. PUID/PGID lets the operator point us at whatever owns their
# media without rebuilding the image.
#
# Defaults preserve prior behavior: 1000:1000 (the baked `fusion` user).
# Set PUID=0 PGID=0 to run as root if your NAS leaves you no choice.
set -eu

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

if [ "$PUID" = "0" ] && [ "$PGID" = "0" ]; then
    # Root requested — chown config dir so the starter-template write works,
    # then exec directly. No gosu hop needed.
    [ -d /etc/fusion ] && chown -R 0:0 /etc/fusion
    exec /usr/local/bin/fusion "$@"
fi

# Reconcile the `fusion` user/group to the requested IDs. groupmod/usermod are
# idempotent — re-running with the same IDs is a no-op.
current_uid="$(id -u fusion)"
current_gid="$(id -g fusion)"
if [ "$current_gid" != "$PGID" ]; then
    groupmod -o -g "$PGID" fusion
fi
if [ "$current_uid" != "$PUID" ]; then
    usermod -o -u "$PUID" fusion
fi

# `/etc/fusion` is the bind-mount target on every documented setup. Chown it
# so the first-run starter-template write succeeds; the bind mount on the
# host is what actually persists, this just fixes in-container permissions.
[ -d /etc/fusion ] && chown -R "$PUID:$PGID" /etc/fusion

exec gosu fusion /usr/local/bin/fusion "$@"

#!/usr/bin/env bash
# install-to-hermes.sh — copies the plugin into ~/.hermes/plugins WITHOUT activating it.
#
# Activation (plugins.enabled + gateway restart) is deliberately NOT part of this script
# and remains an explicit operator action.
set -eu
SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="$HOME/.hermes/plugins/hermes-continuity-capture"

if [ -e "$DEST" ]; then
  echo "Target already exists: $DEST — aborting (no overwrite)." >&2
  exit 1
fi

mkdir -p "$DEST"
cp -r "$SRC/plugin.yaml" "$SRC/__init__.py" "$SRC/continuity_capture" \
      "$SRC/README.md" "$SRC/LICENSE" "$DEST/"
echo "Plugin copied to $DEST (NOT activated yet)."
echo "This plugin does nothing until the operator enables it:"
echo "  hermes config set plugins.enabled '[\"hermes-continuity-capture\", ...]'"
echo "  sudo systemctl restart hermes-gateway.service"

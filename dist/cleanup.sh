#!/bin/sh
# cleanup.sh — drops journal files older than $RETENTION_DAYS (default 30)
# from the brahma state directory.
#
# Cron it:
#
#   30 4 * * *  BRAHMA_STATE_DIR=$HOME/.local/state/brahma \
#               /usr/local/share/brahma/cleanup.sh
#
# Env knobs (all optional):
#
#   BRAHMA_STATE_DIR  — state root (default: $XDG_STATE_HOME/brahma
#                       or ~/.local/state/brahma)
#   RETENTION_DAYS    — drop journal files with mtime older than this (default 30)
#   DRY_RUN           — set to any non-empty value to print what would be
#                       deleted without deleting anything
#
# The script is intentionally simple — extend it here as more cleanup
# steps become useful (worker log rotation, archive pruning, etc).

set -eu

state_dir="${BRAHMA_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/brahma}"
retention="${RETENTION_DAYS:-30}"
journal_dir="$state_dir/journal"

if [ ! -d "$journal_dir" ]; then
	echo "cleanup: $journal_dir does not exist; nothing to do"
	exit 0
fi

action="-delete"
if [ -n "${DRY_RUN:-}" ]; then
	action="-print"
fi

echo "cleanup: pruning *.jsonl in $journal_dir older than $retention days${DRY_RUN:+ (dry run)}"
find "$journal_dir" -type f -name '*.jsonl' -mtime "+$retention" $action

# Drop now-empty pool directories so the layout stays tidy.
if [ -z "${DRY_RUN:-}" ]; then
	find "$journal_dir" -mindepth 1 -type d -empty -delete 2>/dev/null || true
fi
